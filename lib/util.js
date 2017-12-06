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

exports = module.exports = {};

exports.ParseSQL = function (sql, ent){
  var log = global.log;
  if(!log) log = console.log;
  sql = exports.ParseMultiLine(sql);
  if (ent){
    if(sql in ent.SQL) sql = ent.SQL[sql];
    if(sql && (sql.indexOf('%%%')>=0)){
      for(var sqlid in ent.SQL){
        //If the ent.SQL[sqlid] is a function - evaluate the function
        var sqlfunc = ent.SQL[sqlid];
        if(sqlfunc && sqlfunc.params){
          var re=RegExp("%%%"+exports.escapeRegEx(sqlid)+"\\s*\\(.*\\)\\s*%%%",'gm');
          sql = sql.replace(re, function(match){
            var origmatch = match;
            match = match.substr(3+sqlid.length); //Remove function name
            match = match.substr(0,match.length - 3).trim(); //Remove trailing %%%
            match = match.substr(1); //Remove (
            match = match.substr(0,match.length - 1).trim(); //Remove )
            var params = [];
            try{ params = JSON.parse('['+match+']'); }
            catch(ex){
              log('Error parsing: '+origmatch);
              log(ex.toString());
              return '';
            }
            if(params.length != sqlfunc.params.length) throw new Error('SQL Function - invalid argument count: '+origmatch);
            var rsltsql = sqlfunc.sql;
            for(var i=0;i<sqlfunc.params.length;i++){
              rsltsql = exports.ReplaceAll(rsltsql, '%%%'+sqlfunc.params[i]+'%%%', params[i]);
            }
            return rsltsql;
          });
        }
        else sql = exports.ReplaceAll(sql, '%%%'+sqlid+'%%%', sqlfunc);
      }
    }
  }
  return sql;
}
exports.ReplaceAll = function (val, find, replace){
  return val.split(find).join(replace);
}
exports.Size = function (obj) {
  var size = 0, key;
  for (key in obj) {
    if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};
exports.ParseMultiLine = function (val){
  if (!val) return val;
  if (exports.isArray(val)) return val.join(' ');
  return val.toString();
};
exports.isArray = Array.isArray;
exports.str2hex = function(s) {
  var i, l, o = '', n;
  s += '';
  for (i = 0, l = s.length; i < l; i++) {
    n = s.charCodeAt(i)
      .toString(16);
    o += n.length < 2 ? '0' + n : n;
  }
  return o;
}
exports.escapeRegEx = function (q) {
  return q.replace(/[-[\]{}()*+?.,\\/^$|#\s]/g, "\\$&");
}