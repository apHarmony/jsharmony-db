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

function replaceSQLFunc(sql, ent){
  var ftokens = [];
  for(var sqlid in ent.SQL){
    //Convert all functions to arrays of tokens
    var sqlfunc = ent.SQL[sqlid];
    if(sqlfunc && sqlfunc.params){
      try{ ftokens.push({ "name": sqlid, "tokens": SQLScanner.Scan(sqlid) }); }
      catch(ex) {  }
    }
  }
  if(ftokens){
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
      var fsql = applySQLFunc(ent.SQL[ftoken.name], params, src);
      return sql.substr(0,srcStart) + fsql + sql.substr(srcStart+srcLen);
    }
  }
  return sql;
}

function replaceMacro(sql, ent){
  var log = global.log;
  if(!log) log = console.log;

  var sqlids = {};
  for(var sqlid in ent.SQL){
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
        //If the ent.SQL[sqlid] is a function - evaluate the function
        var sqlfunc = ent.SQL[sqlid];
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
                log('Error parsing: '+origmatch);
                log(ex.toString());
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
    lastidx = sql.lastIndexOf('%%%',lastidx-1);
  }
  return sql;
}

exports.ParseSQL = function (sql, ent){
  var log = global.log;
  if(!log) log = console.log;

  sql = exports.ParseMultiLine(sql);
  if (ent){
    if(sql in ent.SQL) sql = ent.SQL[sql];

    var newsql = sql;
    do{
      sql = newsql;
      newsql = replaceMacro(newsql, ent);
      newsql = replaceSQLFunc(newsql, ent);
    } while(newsql != sql);

    //Handle Schema Replacement
    if(ent.Config && ent.Config.schema_replacement){
      for(var i=0;i<ent.Config.schema_replacement.length;i++){
        var sr = ent.Config.schema_replacement[i];
        if(!sr.search_schema) continue;
        var re = new RegExp(sr.search_schema,'gim');
        //Sort matches longest to shortest
        var tables = [];
        while(m = re.exec(sql)){
          if(m.length > 1){
            var table = m[1].toLowerCase();
            if(!_.includes(tables,table)) tables.push(table);
          }
        }
        re.lastIndex = 0;
        tables.sort(function(a,b){ if(a.length>b.length) return -1; if(a.length<b.length) return 1; return 0; });
        _.each(tables, function(table){
          for(var j=0;j<sr.replace_schema.length;j++){
            if(!sr.replace_schema[j].search) continue;
            var res = new RegExp(exports.ReplaceAll(sr.replace_schema[j].search, '%%%TABLE%%%', table),'gim');
            sql = sql.replace(res, function(){
              var rslt = sr.replace_schema[j].replace||'';
              rslt = exports.ReplaceAll(rslt, '%%%TABLE%%%', table);
              for(var k=arguments.length-3;k>=1;k--){
                var exp = arguments[k];
                rslt = exports.ReplaceAll(rslt, '\\'+k.toString(), exp);
              }
              return rslt;
            });
          }
          re.lastIndex = 0;
        });
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