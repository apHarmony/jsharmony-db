/*
Copyright 2017 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var util = require('./util.js');
var async = require('async');
var _ = require('lodash');
var types = require('./DB.types.js');

function DB(){
  if (!global.dbconfig || !global.dbconfig._driver){ throw new Error('Database driver (global.dbconfig._driver) not configured'); }
  else{
		this.name = global.dbconfig._driver.name;
		this.sql = global.dbconfig._driver.sql;
		this.meta = undefined;
		if(global.dbconfig._driver.meta){
			this.meta = new global.dbconfig._driver.meta(this);
		}
	}

  this.parseSQL = function (sql) { return sql; };
}

/*
DB.prototype.ProcessParams = function(params){
	return params.splice(0,2);
}*/

DB.prototype.Recordset = function (context, sql){
  this.Exec(context, sql, 'recordset', arguments);
};

DB.prototype.MultiRecordset = function (context, sql){
  this.Exec(context, sql, 'multirecordset', arguments);
};

DB.prototype.Row = function (context, sql){
  this.Exec(context, sql, 'row', arguments);
};

DB.prototype.Command = function (context, sql){
  this.Exec(context, sql, 'command', arguments);
};

DB.prototype.Scalar = function(context,sql){
  this.Exec(context, sql, 'scalar', arguments);
};

DB.prototype.DBError = function(callback,txt){
	var err = new Error(txt);
	if(callback != null) callback(err,null);
	else throw err;
	return err;	
};

//context,sql
//context,sql,callback
//context,sql,ptypes,params
//context,sql,ptypes,params,callback
//context,sql,ptypes,params,dbtrans,callback
//context,sql,ptypes,params,dbtrans,callback,constring
DB.prototype.Exec = function (context, sql, return_type, args){
	if(typeof context == 'undefined'){ return DB.prototype.DBError(callback,"System Error -- Context not defined."); }
	var params = [];
	var ptypes = [];
  var dbtrans = undefined;
  var constring = undefined;
  sql = this.parseSQL(sql);
	//Process Parameters
  var callback = null;
	if(args.length > 3){
    if (args.length >= 6) {
      dbtrans = args[4];
      callback = args[5];
      if (args.length >= 7) constring = args[6];
    }
		else if(args.length == 5) callback = args[4];
		ptypes = args[2];
		params = args[3];
		if(util.Size(params) != ptypes.length){ return DB.prototype.DBError(callback,"System Error -- Query prepare: Number of parameters does not match number of parameter types.  Check if any parameters are listed twice."); }
		//Convert shortcut parameter types to full form
		if(typeof ptypes == 'string' || ptypes instanceof String){		  
			var i = 0;
			var optypes = [];
			for(var p in params){
				var ptype = ptypes[i];
				var pdbtype = null;
				if(ptype == 's') pdbtype = types.VarChar(p.length);
				else if(ptype == 'i') pdbtype = types.BigInt;
				else if(ptype == 'd') pdbtype = types.Decimal(10,4);
				else { return DB.prototype.DBError(callback,'Invalid type ' + ptype); }
				optypes.push(pdbtype);
				i++;
			}
			ptypes = optypes;
		}
	}
	else if(args.length == 3){ 
	  callback = args[2]; 
	}
	if(return_type=='debug'){ return DB.prototype.DBError(sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params)); }
  
  if (global.debug_params && global.debug_params.db_requests && global.log) {
    global.log(sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params));
  }
  
  if(!constring) constring = global.dbconfig;
  constring._driver.Exec(dbtrans, context, return_type, sql, ptypes, params, callback, constring);
};

DB.prototype.ExecTasks = function (dbtasks, callback){
  //In AppSrv, dbtask = function(dbtrans, callback, transtbl)
  //AppSrv.ExecTasks transforms:
  //  dbtask = function(cb, transtbl){ ... } //with dbtrans set to undefined
  //dbtask = function(cb, rslt){}
  //parallelLimit executes each key/value pair in dbtasks, and creates the rslt object with all the results
  //If parameter 1 is a value (not a function or null/undefined), wait for that function to complete before executing that task

  var dbtasksIsObject = false;
  var dbtasksIsFunctionArray = false;
  var dbtasksIsSeriesArray = false;

  if(!_.isArray(dbtasks)) dbtasksIsObject = true;
  else {
    if(!dbtasks.length || _.isFunction(dbtasks[0])) dbtasksIsFunctionArray = true;
    else dbtasksIsSeriesArray = true;
  }

  //Transform dbtasks to a uniform structure (Array of Collections)
  if(dbtasksIsObject) dbtasks = [dbtasks];
  else if(dbtasksIsFunctionArray) dbtasks = [dbtasks];

  //Prepare to run each array element in parallel, aggregate results, and pass to the next array element
  dbtasks = _.reduce(dbtasks, function(rslt, parallel_dbtasks, key){
    rslt[key] = function(series_rslt, cb){
      //Pass series_rslt to parallel_dbtasks
      parallel_dbtasks = _.transform(parallel_dbtasks, function(rslt, parallel_dbtask, key){
        rslt[key] =  function(cb){ return parallel_dbtask(cb, series_rslt); };
        return rslt;
      });
      //Execute dbtasks
      async.parallelLimit(parallel_dbtasks, 3, function (dberr, parallel_rslt) {
        if(dberr) return cb(dberr, null);
        //Add result to accumulator (series_rslt)
        if(_.isArray(parallel_rslt)){
          var nextidx = 0;
          for(var i=0;i<parallel_rslt.length;i++){
            while(series_rslt[nextidx]) nextidx++;
            series_rslt[nextidx] = parallel_rslt[i];
          }
        }
        else{
          var parallel_overlap = _.pick(parallel_rslt, _.keys(series_rslt));
          if(!_.isEmpty(parallel_overlap)) return cb(new Error('DBTasks - Key '+_.keys(parallel_overlap)[0]+' defined multiple times'));
          series_rslt = _.extend(series_rslt, parallel_rslt);
        }
        cb(null, series_rslt);
      });
    };
    return rslt;
  },[]);
  //Initialize the accumulator (series_rslt)
  dbtasks.unshift(function(cb){
    var series_rslt = {};
    if(dbtasksIsFunctionArray) series_rslt = [];
    return cb(null,series_rslt);
  });
  //Execute operations
  async.waterfall(dbtasks, function(dberr, rslt){
    callback(dberr, (dberr ? null : rslt));
  });
};

DB.prototype.ExecTransTasks = function (dbtasks, callback, constring){
  if (!constring) constring = global.dbconfig;
  //Flatten dbtasks array
  try{
    dbtasks = util.flattenDBTasks(dbtasks);
  }
  catch(ex){
    return callback(ex);
  }
  constring._driver.ExecTransTasks(function(trans, onTransComplete){
    var transtbl = {};
    dbtasks = _.transform(dbtasks, function (rslt, dbtask, key) {
      rslt[key] = function (callback) {
        var xcallback = function (err, rslt) {
          transtbl[key] = rslt;
          callback(err, rslt);
        };
        return dbtask.call(null, trans, xcallback, transtbl);
      };
    });
    async.series(dbtasks, onTransComplete);
  }, callback, constring);
};

DB.prototype.Close = function(callback, constring){
	if(!constring) constring = global.dbconfig;
	constring._driver.Close(callback);
}

DB.types = types;
DB.util = util;
DB.ParseSQL = util.ParseSQL;
DB.log = function(txt){
  if(global.log) global.log(txt);
  else console.log(txt);
}

DB.noDriver = function(){
  this.name = 'none';
  this.sql = new Proxy({}, {
		get: function(target, name){ throw new Error('No global.dbconfig driver specified'); }
	});
  this.con = null;
  this.IsConnected = false;
	this.server = '';
	this.database = '';
	this.user = '';
	this.password = '';
}
DB.noDriver.prototype.Exec = 
DB.noDriver.prototype.ExecTransTasks = 
DB.noDriver.prototype.Close = function(){ throw new Error('No global.dbconfig driver specified'); }

exports = module.exports = DB;