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
var _ = require('lodash');
var SQLScanner = require('./SQL.scanner.js');
var SQLToken = require('./SQL.token.js');

exports = module.exports = {};

function applySQLFunc(sqlfunc, params, src){
  var rslt = '';
  while(sqlfunc.params.length && (params.length < sqlfunc.params.length)) params.push("null");
  if(sqlfunc.params.length && (params.length != sqlfunc.params.length)) throw new Error('SQL Function - invalid argument count: '+src);
  if(sqlfunc.sql){
    rslt = sqlfunc.sql;
    for(var i=0;i<sqlfunc.params.length;i++){
      rslt = exports.ReplaceAll(rslt, '%%%'+sqlfunc.params[i]+'%%%', params[i]);
    }
  }
  else if(sqlfunc.exec){
    var f_config = [];
    for(var i=0;i<sqlfunc.params.length;i++) f_config.push(sqlfunc.params[i]);
    f_config.push(sqlfunc.exec);
    var f = Function.apply(this, f_config);
    rslt = f.apply(this, params);
  }
  return rslt;
}

function replaceSQLFunc(sql, Funcs){
  var ftokens = [];
  for(var sqlid in Funcs){
    //Convert all functions to arrays of tokens
    var sqlfunc = Funcs[sqlid];
    if(sqlfunc && sqlfunc.params){
      try{ ftokens.push({ "name": sqlid, "tokens": SQLScanner.Scan(sqlid) }); }
      catch(ex) {  }
    }
  }
  if(ftokens.length){
    var tokens = [];
    try{
      tokens = SQLScanner.Scan(sql);
    }
    catch(ex){ tokens = []; /* console.log('Error parsing...'); console.log(ex); */ }

    ftokens.sort(function(a,b){ if(a.name.length > b.name.length) return -1; if(a.name.length < b.name.length) return 1; return 0; });

    //Go through all tokens, back to front, and find any matches  schema+DOT+function_name+RPAREN
    var found_function_at = null;
    var ftoken = null;
    for(var i=tokens.length - 1;i >= 0; i--){
      var ctoken = tokens[i];
      if(ctoken.Name=='LPAREN'){
        for(var j=0; j<ftokens.length; j++){
          ftoken = ftokens[j];
          var match = true;
          for(var k=0;k < (ftoken.tokens.length-1); k++){
            var ft = ftoken.tokens[ftoken.tokens.length-k-2];
            if((i-k-1) < 0) {}
            else if(ft.Name==tokens[i-k-1].Name){
              var fval = (ft.Value||'').toString();
              var tval = (tokens[i-k-1].Value||'').toString();
              if(fval.toLowerCase()==tval.toLowerCase()) continue;
            }
            match = false;
            break;
          }
          if(match){
            found_function_at = i-ftoken.tokens.length+1;
            break;
          }
        }
      }
      if(found_function_at !== null) break;
    }
    function parseFunction(idx, name){
      var rslt = new SQLToken.FUNCTION(tokens[idx], name, false, true);
      while(tokens[idx].Name != 'LPAREN') idx++;
      var paramidx = idx;
      idx++;
      while(idx < tokens.length){
        if(tokens[idx].Name == 'LPAREN'){
          var cf = parseFunction(idx, '');
          idx = cf.EndToken;
        }
        else if(tokens[idx].Name == 'COMMA'){
          rslt.ArgumentPos.push(tokens[paramidx].StrPos);
          paramidx = idx;
        }
        else if(tokens[idx].Name == 'RPAREN'){
          if(idx-paramidx > 0) rslt.ArgumentPos.push(tokens[paramidx].StrPos);
          break;
        }
        idx++;
      }
      if(idx == tokens.length) idx--;
      rslt.EndToken = idx;
      rslt.EndPos = tokens[idx].StrPos;
      return rslt;
    }
    if(found_function_at !== null){
      var f = parseFunction(found_function_at, ftoken.name);

      var srcStart = f.StrPos-1;
      var srcLen = f.EndPos-f.StrPos+1;
      var src = sql.substr(srcStart,srcLen);

      var params = [];
      for(var i=0;i<f.ArgumentPos.length;i++){
        var apos = f.ArgumentPos[i];
        if(i==(f.ArgumentPos.length-1)) params.push(sql.substr(apos, f.EndPos-apos-1));
        else params.push(sql.substr(apos, f.ArgumentPos[i+1]-apos-1));
      }
      var fsql = applySQLFunc(Funcs[ftoken.name], params, src);
      return sql.substr(0,srcStart) + fsql + sql.substr(srcStart+srcLen);
    }
  }
  return sql;
}

function replaceMacro(sql, Macros, log){
  var sqlids = {};
  for(var sqlid in Macros){
    //Convert all functions to arrays of tokens
    sqlids[sqlid.toUpperCase()] = true;
  }

  if(!sql) return sql;
  var lastidx = sql.lastIndexOf('%%%');
  while(lastidx >= 0){
    var parenidx = sql.indexOf('(',lastidx+3);
    var pctidx = sql.indexOf('%%%',lastidx+3);
    var fnameidx = parenidx;
    if(pctidx >= 0){
      if(parenidx < 0) fnameidx = pctidx;
      else if(pctidx < parenidx) fnameidx = pctidx;
    }
    if(fnameidx >= 0){
      var sqlid = sql.substr(lastidx+3,fnameidx-lastidx-3).trim();
      if(sqlids[sqlid.toUpperCase()]){
        //If the Macros[sqlid] is a function - evaluate the function
        var sqlfunc = Macros[sqlid];
        if(sqlfunc && sqlfunc.params){
          var endPos = sql.indexOf(')%%%',lastidx);
          if(endPos >= 0){
            var match = sql.substr(lastidx,endPos-lastidx+4);
            found_function = true;
            var origmatch = match;
            match = match.substr(3+sqlid.length); //Remove function name
            match = match.substr(0,match.length - 3).trim(); //Remove trailing %%%
            match = match.substr(1); //Remove (
            match = match.substr(0,match.length - 1).trim(); //Remove )
            var params = [];
            if(sqlfunc.params.length==0) params = [match];
            else {
              try{ params = JSON.parse('['+match+']'); }
              catch(ex){
                log('Error parsing: '+origmatch + ' :: ' + ex.toString());
                return '';
              }
            }
            var sqlfuncrslt = applySQLFunc(sqlfunc, params, origmatch);
            sql = sql.substr(0,lastidx) + sqlfuncrslt + sql.substr(lastidx+origmatch.length);
            lastidx += sqlfuncrslt.length;
          }
        }
        else{
          var endPos = sql.indexOf('%%%',lastidx+3);
          if(endPos >= 0){
            var match = sql.substr(lastidx,endPos-lastidx+3);
            sql = sql.substr(0,lastidx) + sqlfunc + sql.substr(lastidx+match.length);
            lastidx += sqlfunc.length;
          }
        }
      }
    }
    if(lastidx <= 0) lastidx = -1;
    else lastidx = sql.lastIndexOf('%%%',lastidx-1);
  }
  return sql;
}

function replaceSchema(sql, schema_replacement){
  if(!schema_replacement) return sql;

  var schematokens = [];
  for(var i=0;i<schema_replacement.length;i++){
    var schema_replacement_info = schema_replacement[i];
    //Convert all functions to arrays of tokens
    try{ schematokens.push({ "name": schema_replacement_info.search_schema, "tokens": SQLScanner.Scan(schema_replacement_info.search_schema), "replace": schema_replacement_info.replace_schema }); }
    catch(ex) {  }
  }

  if(schematokens.length){
    var tokens = [];
    try{
      tokens = SQLScanner.Scan(sql);
    }
    catch(ex){ tokens = []; /* console.log('Error parsing...'); console.log(ex); */ }

    schematokens.sort(function(a,b){ if(a.name.length > b.name.length) return -1; if(a.name.length < b.name.length) return 1; return 0; });

    //Go through all tokens, back to front, and find any matches
    var found_schema_at = null;
    var schematoken = null;
    for(var i=tokens.length - 1;i >= 0; i--){
      var ctoken = tokens[i];
      if((ctoken.Name=='ID')||(ctoken.Name=='FUNCTION')){
        for(var j=0; j<schematokens.length; j++){
          schematoken = schematokens[j];
          var match = true;
          for(var k=0;k < (schematoken.tokens.length-1); k++){
            var ft = schematoken.tokens[schematoken.tokens.length-k-2];
            if((i-k-1) < 0) {}
            else if(ft.Name==tokens[i-k-1].Name){
              var fval = (ft.Value||'').toString();
              var tval = (tokens[i-k-1].Value||'').toString();
              if(fval.toLowerCase()==tval.toLowerCase()) continue;
            }
            match = false;
            break;
          }
          if(match){
            found_schema_at = i;
            break;
          }
        }
      }
      if(found_schema_at !== null) break;
    }
    
    if(found_schema_at !== null){

      var dbobj = tokens[found_schema_at];

      for(var i=tokens.length - 1;i >= 0; i--){
        var ctoken = tokens[i];
        if((ctoken.Name==dbobj.Name)&&(ctoken.Value.toLowerCase()==dbobj.Value.toLowerCase())){
          var numTokens = 1;
          var srcStart = ctoken.StrPos - 1;
          var srcLen = 0;

          var match = true;
          for(var k=0;k < (schematoken.tokens.length-1); k++){
            var ft = schematoken.tokens[schematoken.tokens.length-k-2];
            if((i-k-1) < 0) {}
            else if(ft.Name==tokens[i-k-1].Name){
              var fval = (ft.Value||'').toString();
              var tval = (tokens[i-k-1].Value||'').toString();
              if(fval.toLowerCase()==tval.toLowerCase()) continue;
            }
            match = false;
            break;
          }
          if(match){
            srcStart = tokens[i - (schematoken.tokens.length - 1)].StrPos - 1;
            srcLen = ctoken.StrPos - srcStart - 1;
            numTokens += schematoken.tokens.length - 1;
          }

          sql = sql.substr(0,srcStart) + schematoken.replace + sql.substr(srcStart+srcLen);
          i -= (numTokens - 1);
        }
      }
      return sql;
    }
  }
  return sql;
}

exports.ParseSQL = function (sql, sqlext, options){
  if(!options) options = { schema_replacement: [], log: console.log };
  sql = exports.ParseMultiLine(sql);
  if (sqlext){
    if(sqlext.Funcs){
      if(sql in sqlext.Funcs) sql = sqlext.Funcs[sql];

      var newsql = sql;
      do{
        sql = newsql;
        newsql = replaceMacro(newsql, sqlext.Funcs, options.log);
        newsql = replaceSQLFunc(newsql, sqlext.Funcs, options.log);
      } while(newsql != sql);
    }

    if(options.schema_replacement && options.schema_replacement.length){
      do{
        sql = newsql;
        newsql = replaceSchema(newsql, options.schema_replacement);
      } while(newsql != sql);
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
exports.flattenDBTasks = function(dbtasks, accumulator){
  if(!accumulator){
    if(_.isArray(dbtasks) && dbtasks.length && _.isFunction(dbtasks[0])) accumulator = [];
    else accumulator = {};
  }
  _.each(dbtasks, function(val, key){
    if(_.isFunction(val)){
      if(key in accumulator) throw new Error('DBTasks - Key '+key+' defined multiple times');
      accumulator[key] = val;
    }
    else exports.flattenDBTasks(val, accumulator);
  });
  return accumulator;
}
exports.waitDefer = function(f, timeout){
  var lastTimeout = null;
  return function(options){
    if(!options) options = {};
    if(lastTimeout) clearTimeout(lastTimeout);
    if(options.clearTimeout){ return; }
    lastTimeout = setTimeout(f, timeout);
  };
}
exports.LogDBResult = function(platform, rslt){ //sql, dbrslt, notices, warnings
  var _LEVEL_WARNING = 2;
  var _LEVEL_INFO = 4;
  var db_log_level = platform.Config.debug_params.db_log_level;
  if(rslt.warnings && (db_log_level & _LEVEL_WARNING)){
    for(var i=0;i<rslt.warnings.length;i++){
      platform.Log.warning('SQL WARNING: '+rslt.warnings[i].message);
    }
  }
  if(rslt.notices && (db_log_level & _LEVEL_INFO)){
    for(var i=0;i<rslt.notices.length;i++){
      platform.Log.info('SQL NOTICE: '+rslt.notices[i].message);
    }
  }
}