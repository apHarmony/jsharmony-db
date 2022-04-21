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
var fs = require('fs');
var path = require('path');
var types = require('./DB.types.js');

function DB(dbconfig, platform){
  if (!dbconfig){ throw new Error('Database dbconfig not configured'); }
  if (!dbconfig._driver){ throw new Error('Database driver (dbconfig._driver) not configured'); }

  this.dbconfig = dbconfig;
  this.SQLExt = new DB.SQLExt();

  //Initialize platform
  if(!platform) platform = this.createPlatform();
  this.setPlatform(platform);

  this.sql = undefined;
  if(dbconfig._driver.sql){ this.sql = new dbconfig._driver.sql(this); }
  this.meta = undefined;
  if(dbconfig._driver.meta){ this.meta = new dbconfig._driver.meta(this); }
  this.schema_definition = { tables: {}, table_schemas: {} };
}

DB.prototype.createPlatform = function(){
  var platform = {
    Log: function(msg){ console.log(msg); }, // eslint-disable-line no-console
    Config: {
      debug_params: {
        db_requests: false,        //Log every database request
        db_raw_sql: false,         //Log raw database SQL
        db_log_level: 6,           //Bitmask: 2 = WARNING, 4 = NOTICES :: Database messages logged to the console / log
        db_error_sql_state: false  //Log SQL state during DB error
      },
      schema_replacement: []
    }
  };
  platform.Log.info = function(msg){ console.log(msg); }; // eslint-disable-line no-console
  platform.Log.warning = function(msg){ console.log(msg); }; // eslint-disable-line no-console
  platform.Log.error = function(msg){ console.log(msg); }; // eslint-disable-line no-console
  return platform;
};

DB.prototype.setPlatform = function(platform){
  this.platform = platform;
  this.dbconfig._driver.platform = platform;
};

DB.prototype.isSilent = function(){
  if(!this.dbconfig || !this.dbconfig._driver) return false;
  return this.dbconfig._driver.silent;
};

DB.prototype.setSilent = function(silent){
  if(!this.dbconfig || !this.dbconfig._driver) return false;
  this.dbconfig._driver.silent = silent;
};

DB.prototype.getSQLExt = function(){
  return this.SQLExt;
};

DB.prototype.getDefaultSchema = function(){
  if(!this.dbconfig || !this.dbconfig._driver) return undefined;
  return this.dbconfig._driver.getDefaultSchema();
};

DB.prototype.getType = function(){
  if(!this.dbconfig || !this.dbconfig._driver) return undefined;
  return this.dbconfig._driver.name;
};

DB.prototype.getTableDefinition = function(table_name){
  var _this = this;
  if(!table_name) return undefined;
  if(!this.dbconfig || !this.dbconfig._driver) return undefined;
  table_name = this.ParseSQL(table_name);
  var defaultSchema = _this.getDefaultSchema();
  table_name = (table_name||'').toLowerCase();
  if(table_name in _this.schema_definition.tables) return _this.schema_definition.tables[table_name];
  if(table_name in _this.schema_definition.table_schemas){
    var table_schemas = _this.schema_definition.table_schemas[table_name];
    var schema = table_schemas[0];
    if(table_schemas.indexOf(defaultSchema)>=0) schema = defaultSchema;
    return _this.schema_definition.tables[schema+'.'+table_name];
  }
  return undefined;
};

DB.prototype.getFieldDefinition = function(table_name,field_name,table){
  var _this = this;
  if(!table) table =  _this.getTableDefinition(table_name);
  if(!table) return undefined;
  field_name = (field_name||'').toLowerCase();
  return table.fields[field_name];
};

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
//context,sql,ptypes,params,dbtrans,callback,dbconfig
//context,sql,ptypes,params,dbtrans,callback,{ dbconfig, sqlfuncs }
DB.prototype.Exec = function (context, sql, return_type, args){
  if(typeof context == 'undefined'){ return DB.prototype.DBError(callback,'System Error -- Context not defined for: '+sql); }
  var params = [];
  var ptypes = [];
  var dbtrans = undefined;
  var dbparams = undefined;
  var sqlFuncs = undefined;

  //Process Parameters
  var callback = null;
  if(args.length > 3){
    if (args.length >= 6) {
      dbtrans = args[4];
      callback = args[5];
      if (args.length >= 7) dbparams = args[6];
    }
    else if(args.length == 5) callback = args[4];
    ptypes = args[2];
    params = args[3];
    if(util.Size(params) != ptypes.length){ return DB.prototype.DBError(callback,'System Error -- Query prepare: Number of parameters does not match number of parameter types.  Check if any parameters are listed twice.'); }
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
  
  //Parse SQL + Replace Funcs / Macros
  if(dbparams && dbparams.sqlFuncs) sqlFuncs = dbparams.sqlFuncs;
  sql = this.ParseSQL(sql, sqlFuncs);

  if(return_type=='debug'){ return DB.prototype.DBError(sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params)); }
  
  if (this.platform.Config.debug_params && this.platform.Config.debug_params.db_requests && this.platform.Log) {
    this.platform.Log.info(sql + ' ' + JSON.stringify(ptypes) + ' ' + JSON.stringify(params), { source: 'database' });
  }
  
  var dbconfig = null;
  if(dbparams && dbparams.dbconfig) dbconfig = dbparams.dbconfig;
  else if(dbparams && dbparams._driver) dbconfig = dbparams;
  else if(this.dbconfig) dbconfig = this.dbconfig;
  else { return DB.prototype.DBError(callback,'System Error -- No database driver defined'); }
  dbconfig._driver.Exec(dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig);
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

  if(!_.isArray(dbtasks)) dbtasksIsObject = true;
  else {
    if(!dbtasks.length || _.isFunction(dbtasks[0])) dbtasksIsFunctionArray = true;
  }

  //Transform dbtasks to a uniform structure (Array of Collections)
  if(dbtasksIsObject) dbtasks = [dbtasks];
  else if(dbtasksIsFunctionArray) dbtasks = [dbtasks];

  //Prepare to run each array element in parallel, aggregate results, and pass to the next array element
  dbtasks = _.reduce(dbtasks, function(rslt, parallel_dbtasks, key){
    rslt[key] = function(series_rslt, cb){
      //Pass series_rslt to parallel_dbtasks
      parallel_dbtasks = _.transform(parallel_dbtasks, function(rslt, parallel_dbtask, key){
        //Pass series_rslt to each parallel_dbtask, so that each set of parallel requests can access data from a previous series
        rslt[key] = function(cb){
          //Transform arguments to return stats, if omitted by client
          var cb_fullargs = function(err, rslt, stats){ return cb(err, rslt, stats); };
          return parallel_dbtask(cb_fullargs, series_rslt.rslt);
        };
        return rslt;
      });
      //Execute dbtasks
      async.parallelLimit(parallel_dbtasks, 3, function (dberr, parallel_rslt) {
        if(dberr) return cb(dberr, null);
        //Add result to accumulator (series_rslt)
        if(_.isArray(parallel_rslt)){
          var nextidx = 0;
          for(var i=0;i<parallel_rslt.length;i++){
            while(nextidx in series_rslt.rslt) nextidx++;
            series_rslt.rslt[nextidx] = ((parallel_rslt[i].length >= 1) ? parallel_rslt[i][0] : undefined);
            series_rslt.stats[nextidx] = ((parallel_rslt[i].length >= 2) ? parallel_rslt[i][1] : {} );
          }
        }
        else{
          var parallel_overlap = _.pick(parallel_rslt, _.keys(series_rslt.rslt));
          if(!_.isEmpty(parallel_overlap)) return cb(new Error('DBTasks - Key '+_.keys(parallel_overlap)[0]+' defined multiple times'));
          for(var key in parallel_rslt){
            series_rslt.rslt[key] = ((parallel_rslt[key].length >= 1) ? parallel_rslt[key][0] : undefined);
            series_rslt.stats[key] = ((parallel_rslt[key].length >= 2) ? parallel_rslt[key][1] : {} );
          }
        }
        cb(null, series_rslt);
      });
    };
    return rslt;
  },[]);
  //Initialize the accumulator (series_rslt)
  dbtasks.unshift(function(cb){
    var series_rslt = { rslt: {}, stats: {}};
    if(dbtasksIsFunctionArray) series_rslt = { rslt: [], stats: [] };
    return cb(null,series_rslt);
  });
  //Execute operations
  async.waterfall(dbtasks, function(dberr, rslt){
    callback(dberr, (dberr ? null : rslt.rslt), (dberr ? null : rslt.stats));
  });
};

DB.prototype.ExecTransTasks = function (dbtasks, callback, dbconfig){
  if (!dbconfig) dbconfig = this.dbconfig;
  //Flatten dbtasks array
  try{
    dbtasks = util.flattenDBTasks(dbtasks);
  }
  catch(ex){
    return callback(ex);
  }
  //Execute transaction
  dbconfig._driver.ExecTransTasks(function(trans, onTransComplete){
    var transtbl = {};
    dbtasks = _.transform(dbtasks, function (rslt, dbtask, key) {
      rslt[key] = function (callback) {
        var xcallback = function (err, rslt, stats) {
          transtbl[key] = rslt;
          callback(err, rslt, stats);
        };
        return dbtask.call(null, trans, xcallback, transtbl);
      };
    });
    async.series(dbtasks, onTransComplete);
  }, function(err, rslt, stats){
    var dbrslt = {};
    var dbstats = {};
    if(_.isArray(rslt)){
      dbrslt = [];
      dbstats = [];
    }
    for(var key in rslt){
      dbrslt[key] = rslt[key][0];
      dbstats[key] = rslt[key][1];
    }
    callback(err, dbrslt, dbstats);
  }, dbconfig);
};

DB.prototype.Close = function(callback, dbconfig){
  if(!dbconfig) dbconfig = this.dbconfig;
  dbconfig._driver.Close(callback);
};

DB.prototype.RunScripts = function(jsh, scriptid, options, cb){
  if(!scriptid) scriptid = [];

  //scriptid
  //  [] = All Scripts
  //  ['jsharmony-factory'] = All jsHarmony Factory Scripts
  //  ['jsharmony-factory','init'] == All jsHarmony Factory Init Scripts
  //  ['jsharmony-factory','init','create'] == All jsHarmony Factory Init Create DB Scripts
  //  ['*', 'init'] = Init scripts for each module

  function SQLNode(sql, module, desc){
    this.sql = sql;
    this.module = module;
    this.desc = desc || '';
  }

  function findSQLScripts(node, scriptid, module, nodepath){
    var rslt = null;
    //Return current node if matched
    if(scriptid===null){
      if(_.isString(node)) return new SQLNode(node, module, nodepath);
      rslt = {};
      for(let key in node){
        rslt[key] = findSQLScripts(node[key], null, module, nodepath + '/' + key);
      }
      return rslt;
    }
    else if(scriptid.length==0){
      return [findSQLScripts(node, null, module, nodepath)];
    }
    //Search child nodes for scriptid
    rslt = [];
    for(let key in node){
      var child = node[key];
      if((scriptid[0]=='*')||(scriptid[0]==key)) rslt = rslt.concat(findSQLScripts(child, scriptid.slice(1), (typeof module == 'undefined' ? key : module), nodepath + '/' + key));
    }
    return rslt;
  }

  function flattenSQLScripts(node){
    var rslt = {};
    for(var key in node){
      var val = node[key];
      if(!val) continue;
      if(val instanceof SQLNode){
        if(!(key in rslt)) rslt[key] = [];
        rslt[key] = rslt[key].concat(val);
      }
      else {
        var flatnode = flattenSQLScripts(val);
        for(var flatkey in flatnode){
          var newkey = key+'.'+flatkey;
          if(!(newkey in rslt)) rslt[newkey] = [];
          rslt[newkey] = rslt[newkey].concat(flatnode[flatkey]);
        }
      }
    }
    return rslt;
  }

  function sortNestedScripts(node){
    if(node instanceof SQLNode) return node;
    if(_.isArray(node)){
      for(let i=0;i<node.length;i++){
        node[i] = sortNestedScripts(node[i]);
      }
      return node;
    }
    var rslt = {};
    var keys = _.keys(node);
    keys = keys.sort();
    for(let i=0;i<keys.length;i++){
      rslt[keys[i]] = sortNestedScripts(node[keys[i]]);
    }
    return rslt;
  }

  //-----------------------------

  //Search Scripts tree for target scriptid
  var sqlext = this.getSQLExt();
  var sqlscripts = findSQLScripts(sqlext.Scripts, scriptid, undefined, 'scripts');
  sqlscripts = sortNestedScripts(sqlscripts);

  if(_.isEmpty(sqlscripts)){
    jsh.Log.error('No scripts found for script ID: '+scriptid.join('.'));
    return cb(null, null, null);
  }
  var flatscripts = {};

  //Flatten SQLScripts result into array
  for(let i=0;i<sqlscripts.length;i++){
    sqlscripts[i] = flattenSQLScripts(sqlscripts[i]);
  }
  for(let i=0;i<sqlscripts.length;i++){
    for(let key in sqlscripts[i]){
      if(!(key in flatscripts)) flatscripts[key] = [];
      flatscripts[key] = flatscripts[key].concat(sqlscripts[i][key]);
    }
  }

  //Sort scripts (Prepend __START__ scripts)
  var flatkeys = _.keys(flatscripts);
  var sortedkeys = [];
  var startkeys = [];
  for(let i=0;i<flatkeys.length;i++){
    let key = flatkeys[i];
    if(key.indexOf('__START__')>=0) startkeys.push(key);
    else sortedkeys.push(key);
  }
  sortedkeys = startkeys.concat(sortedkeys);

  var dbscripts = {};
  for(let i=0;i<sortedkeys.length;i++) dbscripts[sortedkeys[i]] = flatscripts[sortedkeys[i]];

  //RunScriptArray
  return this.RunScriptArray(jsh, dbscripts, options, cb);
};

DB.prototype.getObjectDiff = function(jsh, sqlext, moduleName, cb){
  var _this = this;
  var sql = '';

  var module = null;
  if(jsh && jsh.Modules && moduleName){
    module = jsh.Modules[moduleName];
  }
  if(!module) return cb(new Error('Module not found: '+moduleName));

  if(sqlext.Objects && sqlext.Objects[moduleName] && sqlext.Objects[moduleName].length && _this.sql && _this.sql.object){
    
    //Sort objects
    var sqlobjects_code = [];
    var sqlobjects_table = [];
    var sqlobjects_view = [];
    var sqlobjects_other = [];
    _.each(sqlext.Objects[moduleName], function(sqlobject){
      if((sqlobject.type=='code') || (sqlobject.type=='code2')) sqlobjects_code.push(sqlobject);
      else if(sqlobject.type=='table') sqlobjects_table.push(sqlobject);
      else if(sqlobject.type=='view') sqlobjects_view.push(sqlobject);
      else  sqlobjects_other.push(sqlobject);
    });

    sqlobjects_code.sort(function(a,b){
      if(a.name > b.name) return 1;
      if(a.name < b.name) return -1;
      return 0;
    });

    sql += '\r\n\r\n';
    sql += 'SQL Object Tables\r\n';
    sql += '-----------------\r\n';
    _.map(sqlobjects_table, function(table){ sql += table.name + '\r\n'; });

    sql += '\r\n\r\n';
    sql += 'DB Tables\r\n';
    sql += '---------\r\n';
    _.map(_this.schema_definition.tables, function(table){ sql += JSON.stringify(table,null,4) + '\r\n'; });

    //1. Clone sqlobjects
    //2. Remove unrelated tables from schema_definition (based on module.schema)
    //3. Apply schema + schema replacement to sqlobjects
    //     table.fullname
    //4. Compare based on table.fullname
    //-------
    //5. Columns
    //6. Indexes, constraints, foreign keys


    //1. Get schema for module (or default schema if blank)
    //2. Figure out how to convert schema for SQLite, for comparison
    //3. Create three arrays - existing_tables, new_tables, deleted_tables
    //4. For each existing_table: existing_fields, new_fields, deleted_fields
    //5. For each existing_table: existing_primary_keys, new_primary_keys, delete_primary_keys
    //6. ... foreign keys (individual + multiple)
    //7. ... unique (individual + multiple)
    //9. ... index (individual + multiple)
    //10. Sort tables by dependencies (from getObjectSQL)
    //11. Generator
    //12. Modify unique + indexes so that they are named constraints (+ possibly foreign / primary keys)
  }

  return cb(null, sql);
};

DB.prototype.getObjectSQL = function(jsh, sqlext, moduleName, module, sql, dbconfig){
  var _this = this;
  var rslt = '';

  if(sqlext.Objects && sqlext.Objects[moduleName] && sqlext.Objects[moduleName].length && _this.sql && _this.sql.object){

    //Generate merged index of all tables and views
    var object_idx = {};
    _.each(sqlext.Objects, function(moduleObjects, _moduleName){
      _.each(moduleObjects, function(sqlobject){
        if(!(sqlobject.name in object_idx)) object_idx[sqlobject.name] = {};
        _.merge(object_idx[sqlobject.name], sqlobject);
      });
    });

    //Sort objects
    var sqlobjects_code = [];
    var sqlobjects_table = [];
    var sqlobjects_view = [];
    var sqlobjects_other = [];
    _.each(sqlext.Objects[moduleName], function(sqlobject){
      var objname = sqlobject.name;
      if((sqlobject.type=='code') || (sqlobject.type=='code2')) sqlobjects_code.push(sqlobject);
      else if((sqlobject.type=='table') || (object_idx[objname] && (object_idx[objname].type=='table'))){
        sqlobjects_table.push(sqlobject);
      }
      else if((sqlobject.type=='view') || (object_idx[objname] && (object_idx[objname].type=='view'))){
        sqlobjects_view.push(sqlobject);
      }
      else {
        sqlobjects_other.push(sqlobject);
      }
    });

    sqlobjects_code.sort(function(a,b){
      if(a.name > b.name) return 1;
      if(a.name < b.name) return -1;
      return 0;
    });

    var genDependencies = function(tbl, col, dependencyKeys){
      if(tbl.name in col) return col[tbl.name];
      col[tbl.name] = {}; //Prevent loops
      var dependencies = {};
      _.each(dependencyKeys, function(dependencyKey){
        for(var depname in tbl[dependencyKey]){
          if(depname == tbl.name) continue;
          var dep = object_idx[depname];
          if(!dep) continue;
          dependencies[depname] = tbl[dependencyKey][depname];
          var depdep = genDependencies(dep, col, dependencyKeys);
          for(var key in depdep){ if(!(key in dependencies)) dependencies[key] = depdep[key]; }
        }
      });
      col[tbl.name] = dependencies;
      return dependencies;
    };

    var sortDependencies = function(col, dependencyKeys){
      var rslt = [];
      var depcol = {};
      var allNames = {};
      _.each(col, function(tbl){
        allNames[tbl.name] = tbl;
        genDependencies(tbl, depcol, dependencyKeys);
      });
      _.each(_.keys(depcol), function(depname){
        var dep = depcol[depname];
        _.each(_.keys(dep), function(deptbl){
          if(!(deptbl in allNames)) delete dep[deptbl];
        });
        if(!(depname in allNames)) delete depcol[depname];
      });
      var foundMatch = true;
      while(foundMatch){
        foundMatch = false;
        _.each(_.keys(depcol), function(depname){
          var dep = depcol[depname];
          if(_.isEmpty(dep)){
            foundMatch = true;
            rslt.push(allNames[depname]);
            delete depcol[depname];
            _.each(depcol, function(subdep){
              delete subdep[depname];
            });
          }
        });
      }
      for(var key in depcol){
        jsh.Log.error('LOOP processing database object: '+key);
        rslt.push(allNames[key]);
      }
      return rslt;
    };

    //Sort tables by dependencies
    var sqlobjects_table_sorted = sortDependencies(sqlobjects_table, ['_foreignkeys','_dependencies']);

    //Sort views by dependencies
    var sqlobjects_view_sorted = sortDependencies(sqlobjects_view, ['_tables']);

    //Parse View Columns
    var tables = {};
    _.each(sqlext.Objects, function(moduleObjects, moduleName){
      _.each(moduleObjects, function(obj){
        if((obj.type=='table') && obj.name && obj.columns){
          var cols = {};
          _.each(obj.columns, function(col){ cols[col.name] = col; });
          tables[obj.name] = cols;
        }
      });
    });
    _.each(sqlext.Objects[moduleName], function(obj){
      if(obj.type=='view'){
        obj.columns = [];
        var cols = {};
        if(obj.tables) for(var table in obj.tables){
          if(!tables[table]) { continue; }
          _.each(obj.tables[table].columns, function(col){
            if(col && col.name){
              if(col.name in tables[table]){
                var tablecol = _.extend({}, tables[table][col.name]);
                delete tablecol.default;
                obj.columns.push(tablecol);
              }
              else obj.columns.push(col);
            }
          });
        }
        _.each(obj.columns, function(col){ cols[col.name] = col; });
        tables[obj.name] = cols;
      }
    });

    //Create aggregate array of objects
    var sqlobjects = sqlobjects_other.concat(sqlobjects_code).concat(sqlobjects_table_sorted).concat(sqlobjects_view_sorted);
    //Reverse the array for drop operations
    if((sql=='drop')||(sql=='restructure_drop')){
      sqlobjects = sqlobjects.reverse();
    }


    if(sql=='init'){
      if(_this.sql.object.initSchema) rslt += _this.sql.object.initSchema(jsh, module, dbconfig) + '\n';
    }

    for(let i=0;i<sqlobjects.length;i++){
      let obj = sqlobjects[i];
      if(obj.type=='view'){
        if(sql=='restructure_init'){
          rslt += _this.sql.object.init(jsh, module, obj) + '\n';
        }
      }
    }
    
    for(let i=0;i<sqlobjects.length;i++){
      let obj = sqlobjects[i];
      if(sql=='init'){
        if(obj.type!='view') rslt += _this.sql.object.init(jsh, module, obj) + '\n';
      }
      else if(sql=='init_data'){
        rslt += _this.sql.object.initData(jsh, module, obj) + '\n';
      }
      else if(sql=='sample_data'){
        rslt += _this.sql.object.sampleData(jsh, module, obj) + '\n';
        if(obj.sample_data_files){
          _.each(obj.sample_data_files, function(copy_cmd){
            for(var src in copy_cmd){
              rslt += '%%%copy_file:'+path.join(path.dirname(obj.path),'data_files',src)+'>'+path.join(jsh.Config.datadir,copy_cmd[src])+'%%%\n';
            }
          });
        }
      }
      else if(sql=='restructure_init'){
        rslt += _this.sql.object.restructureInit(jsh, module, obj) + '\n';
      }
      else if(sql=='restructure_drop'){
        rslt += _this.sql.object.restructureDrop(jsh, module, obj) + '\n';
      }
      else if(sql=='drop'){
        rslt += _this.sql.object.drop(jsh, module, obj) + '\n';
      }
    }

    for(let i=0;i<sqlobjects.length;i++){
      let obj = sqlobjects[i];
      if(obj.type=='view'){
        if(sql=='restructure_drop'){
          rslt += _this.sql.object.drop(jsh, module, obj) + '\n';
        }
      }
    }

    if(sql=='drop'){
      if(_this.sql.object.dropSchema) rslt += _this.sql.object.dropSchema(jsh, module) + '\n';
    }
  }
  return rslt;
};

DB.prototype.RunScriptArray = function(jsh, dbscripts, options, cb){
  options = _.extend({
    onSQL: function(dbscript_name, bi, sql){ },
    onSQLResult: function(err, rslt, sql){ },
    dbconfig: this.dbconfig,
    sqlFuncs: undefined,
    context: ''
  }, options||{});

  var _this = this;
  var dbscript_names = [];
  for(var key in dbscripts) dbscript_names.push(key);

  //Execute scripts
  var dbrslt = [];
  var dbstats = [];
  var sqlext = _this.getSQLExt();
  var copy_files = [];

  async.eachSeries(dbscript_names, function(dbscript_name, db_cb){
    var sqlnodes = dbscripts[dbscript_name];
    if(!_.isArray(sqlnodes)) sqlnodes = [sqlnodes];

    var bsql = '';
    for(var i=0;i<sqlnodes.length;i++){
      var sqlnode = sqlnodes[i];
      if(!sqlnode.sql) continue;
      var sql = sqlnode.sql;

      var module = null;
      if(jsh && jsh.Modules && sqlnode.module){
        module = jsh.Modules[sqlnode.module];
      }


      if(sql.substr(0,7)=='object:'){
        try{
          sql = _this.getObjectSQL(jsh, sqlext, sqlnode.module, module, sql.substr(7), options.dbconfig);
        }
        catch(ex){
          return db_cb(ex);
        }
      }

      //Run ParseSQLFuncs separately for each multi-line string
      
      if(module){
        sql = module.transform.Apply(sql, sqlnode.desc);
        sql = util.ReplaceAll(sql,'%%%NAMESPACE%%%',module.namespace);
        sql = util.ReplaceAll(sql,'%%%SCHEMA%%%',module.schema?module.schema+'.':'');
      }
      if(options.sqlFuncs) sql = _this.ParseSQLFuncs(sql, options.sqlFuncs);
      sql = _this.ParseSQL(sql);

      if(bsql) bsql += ' ';
      bsql += sql;
    }

    bsql = _this.sql.ParseBatchSQL(bsql);
    var bi = 0;

    async.eachSeries(bsql, function(sql, sql_cb){
      bi++;
      if(options.onSQL(dbscript_name, bi, sql)===false) return sql_cb();
      _this.MultiRecordset(options.context,sql,[],{},undefined,function(err,rslt,stats){
        options.onSQLResult(err, rslt, sql);
        dbrslt.push(rslt);
        dbstats.push(stats);
        _.each(rslt, function(rs){
          _.each(rs, function(row){
            for(var key in row){
              var msg = row[key];
              if(msg && _.isString(msg)){
                //Extract Files to be Copied
                msg = msg.replace(/%%%copy_file:([^%]*)%%%/g, function(match, p1){
                  copy_files.push(p1.split('>'));
                  return '';
                });
              }
            }
          });
        });
        if(err){ return db_cb(err); }
        return sql_cb();
      }, options.dbconfig);
    }, function(){
      if(db_cb) return db_cb();
    });
  }, function(err){
    if(err){ return cb(err); }
    //Copy Files
    async.eachSeries(copy_files, function(copy_file, copy_file_cb){
      if(copy_file.length != 2) throw new Error('Invalid copy file command - must have two arguments [src, dest]: '+JSON.stringify(copy_file));
      var srcpath = copy_file[0];
      var dstpath = copy_file[1];
      fs.copyFile(srcpath, dstpath, copy_file_cb);
    }, function(err){
      if(err){ return cb(err); }
      return cb(null, dbrslt, dbstats);
    });
  });
};

DB.prototype.RunScriptsInFolder = function(jsh, fpath, options, cb){
  options = _.extend({
    prefix: ''
  }, options||{});
  var _this = this;
  var dbscripts = {};
  var dbscript_names = [];

  //Read scripts from folder
  fs.readdir(fpath, function(err, files){
    if(err) return cb(err);
    files.sort();
    for (var i = 0; i < files.length; i++) {
      var fname = files[i];
      if (fname.indexOf('.sql', fname.length - 4) == -1) continue;
      if (options.prefix && (fname.substr(0,options.prefix.length) != (options.prefix))) continue;
      dbscript_names.push(fname);
    }
    async.eachLimit(dbscript_names, 5, function(read_cb){
      fs.readFile(path.join(fpath + '/' + fname),'utf8', function(err, data){
        if(err) return read_cb(err);
        dbscripts[fname] = data;
        read_cb();
      });
    }, function(err){
      if(err) return cb(err);
      return _this.RunScripts(jsh, dbscripts, options, cb);
    });
  });
};

DB.prototype.ParseSQL = function(sql, sqlFuncs, context){
  if(sqlFuncs) sql = this.ParseSQLFuncs(sql, sqlFuncs, context);
  return DB.ParseSQLBase(sql, this.platform, this.SQLExt, context);
};

DB.ParseSQLBase = function(sql, platform, sqlext, context){
  return util.ParseSQL(sql, sqlext, {
    schema_replacement: platform.Config.schema_replacement,
    log: function(txt){ platform.Log.error(txt, { source: 'database' }); },
    context: context
  });
};

DB.prototype.applySQLParams = function(sql, ptypes, params, dbconfig){
  if(!dbconfig) dbconfig = this.dbconfig;
  return dbconfig._driver.applySQLParams(sql, ptypes, params);
};

DB.prototype.ParseSQLFuncs = function(sql, sqlFuncs, context){
  var sqlext = new DB.SQLExt();
  sqlext.Funcs = sqlFuncs;
  var sqlplatform = this.createPlatform();
  sqlplatform.Log = this.platform.Log;
  return DB.ParseSQLBase(sql, sqlplatform, sqlext, context);
};

DB.prototype.Log = function(txt){
  if(this.platform.Log) this.platform.Log.error(txt, { source: 'database' });
  else console.log(txt); // eslint-disable-line no-console
};

DB.prototype.util = util;

DB.types = types;
DB.util = util;

DB.noDriver = function(){
  this.name = 'none';
  this.sql = function(){
    return new Proxy({}, {
      get: function(target, name){ throw new Error('No db driver configured'); }
    });
  };
  this.con = null;
  this.IsConnected = false;
  this.server = '';
  this.database = '';
  this.user = '';
  this.password = '';
};
DB.noDriver.prototype.Exec =
DB.noDriver.prototype.ExecTransTasks =
DB.noDriver.prototype.Close = function(){ throw new Error('No db driver configured'); };
DB.noDriver.prototype.getDefaultSchema =
DB.noDriver.prototype.getTableDefinition = function(){ return undefined; };

DB.SQLExt = function(){
  this.Funcs = {};
  this.Scripts = {};
  this.Objects = {};
  this.CustomDataTypes = {};
  this.Meta = {
    FuncSource: {}
  };
};

DB.TransactionConnectionId = 0;
DB.TransactionConnection = function(con, dbconfig){
  DB.TransactionConnectionId++;
  this.id = DB.TransactionConnectionId;
  this.con = con;
  this.dbconfig = dbconfig;
};

DB.Message = function(_severity, _message){
  this.severity = _severity || DB.Message.NOTICE;
  this.message = _message || DB.Message.NOTICE;
};
DB.Message.prototype.toString = function(){
  return this.severity + ' ' + this.message;
};
DB.Message.ERROR = 'ERROR';
DB.Message.WARNING = 'WARNING';
DB.Message.NOTICE = 'NOTICE';

exports = module.exports = DB;