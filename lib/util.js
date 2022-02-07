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

function applySQLFunc(sqlfunc, params, src, context){
  var rslt = '';
  var variable_arguments = !sqlfunc.params.length;
  if((params.length==1) && (params[0]==='')) params = [];
  for(let i=0;i<sqlfunc.params.length;i++){
    if(sqlfunc.params[i]=='...'){ variable_arguments = true; continue; }
    if(params.length > i) continue;
    params.push('null');
  }
  if(!variable_arguments && (params.length != sqlfunc.params.length)) throw new Error('SQL Function - invalid argument count: ' + params.length + ', expected ' + sqlfunc.params.length + ': '+src);
  if((typeof sqlfunc.sql != 'undefined') && (sqlfunc.sql !== null)){
    rslt = exports.ParseMultiLine(sqlfunc.sql);
    var variable_arg = null;
    for(let i=0;i<sqlfunc.params.length;i++){
      if(sqlfunc.params[i]=='...'){
        if(variable_arg===null) variable_arg = '';
        else variable_arg += ',';
        variable_arg = params[i];
      }
      rslt = exports.ReplaceAll(rslt, '%%%'+sqlfunc.params[i]+'%%%', params[i]);
    }
    if(variable_arg!==null) rslt = exports.ReplaceAll(rslt, '%%%...%%%', variable_arg);
  }
  else if(sqlfunc.exec){
    var f_config = [];
    var f_exec = exports.ParseMultiLine(sqlfunc.exec);
    for(let i=0;i<sqlfunc.params.length;i++){
      if(sqlfunc.params[i]=='...') continue;
      f_config.push(sqlfunc.params[i]);
    }
    f_config.push(f_exec);
    try{
      var f = Function.apply(context, f_config);
    }
    catch(ex){
      ex.message = 'Error compiling '+f_exec+' ::\n\n '+(ex.message||'');
      throw(ex);
    }
    try{
      rslt = f.apply(context, params);
    }
    catch(ex){
      var errmsg = 'Error executing:\n';
      errmsg += src+'\n';
      errmsg +='::\n'+(ex.message||'');
      ex.message = errmsg;
      throw(ex);
    }
  }
  return rslt;
}

function replaceSQLFunc(sql, Funcs, context){
  var ftokens = [];
  var rslt = {
    sql: sql,
    stats: {
      count: 0
    }
  };
  for(var sqlid in Funcs){
    //Convert all functions to arrays of tokens
    var sqlfunc = Funcs[sqlid];
    if(sqlfunc && sqlfunc.params){
      try{ ftokens.push({ 'name': sqlid, 'tokens': SQLScanner.Scan(sqlid) }); }
      catch(ex) { /* Do nothing */ }
    }
  }
  if(ftokens.length){
    var tokens = [];
    try{
      tokens = SQLScanner.Scan(rslt.sql);
    }
    catch(ex){ tokens = []; /* console.log('Error parsing...'); console.log(ex); */ }

    //Sort functions by length descending
    ftokens.sort(function(a,b){ if(a.name.length > b.name.length) return -1; if(a.name.length < b.name.length) return 1; return 0; });

    //Get function arguments
    var parseFunction = function(idx, name){
      var rslt = new SQLToken.FUNCTION(tokens[idx], name, false, true);
      while(tokens[idx].Name != 'LPAREN'){
        idx++;
        if(idx >= tokens.length) throw new Error('Missing opening parenthesis in '+sql);
      }
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
    };

    var found_function_at = null;
    var last_token_index = tokens.length;

    do{
      
      //Go through all tokens, back to front, and find any matches  schema+DOT+function_name+RPAREN
      var ftoken = null;
      found_function_at = null;
      for(var i=last_token_index - 1;i >= 0; i--){
        var ctoken = tokens[i];
        if(!ctoken){ throw new Error('Error parsing SQL: ' + sql); }
        if(ctoken.Name=='LPAREN'){
          for(var j=0; j<ftokens.length; j++){
            ftoken = ftokens[j];
            var match = true;
            for(var k=0;k < (ftoken.tokens.length-1); k++){
              var ft = ftoken.tokens[ftoken.tokens.length-k-2];
              if((i-k-1) < 0) { /* Do nothing */ }
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

      if(found_function_at !== null){
        last_token_index = i;
        var f = parseFunction(found_function_at, ftoken.name);

        var srcStart = f.StrPos-1;
        var srcLen = f.EndPos-f.StrPos+1;
        var src = rslt.sql.substr(srcStart,srcLen);

        //If function has unresolved macros, continue
        if(src.indexOf('%%%')>=0) break;

        var params = [];
        for(let i=0;i<f.ArgumentPos.length;i++){
          var apos = f.ArgumentPos[i];
          if(i==(f.ArgumentPos.length-1)) params.push(rslt.sql.substr(apos, f.EndPos-apos-1));
          else params.push(rslt.sql.substr(apos, f.ArgumentPos[i+1]-apos-1));
        }
        var fsql = applySQLFunc(Funcs[ftoken.name], params, src, context);
        rslt.sql = rslt.sql.substr(0,srcStart) + fsql + rslt.sql.substr(srcStart+srcLen);

        //Update tokens
        var tokenFrom = found_function_at;
        var tokenTo = f.EndToken;

        //Update all tokens after the function
        var lenchange = fsql.length - srcLen;
        for(let i=(tokenTo+1); i < tokens.length; i++){
          tokens[i].StrPos += lenchange;
        }

        //Remove and reparse function tokens
        var newtokens = [];
        try{
          newtokens = SQLScanner.Scan(fsql);
        }
        catch(ex){ newtokens = []; /* console.log('Error parsing...'); console.log(ex); */ }

        for(let i=0;i<newtokens.length;i++){
          newtokens[i].StartPos += tokens[tokenFrom].StartPos - 1;
          newtokens[i].StartLine += tokens[tokenFrom].StartLine - 1;
          newtokens[i].Line += tokens[tokenFrom].StartPos - 1;
          newtokens[i].Pos += tokens[tokenFrom].StartLine - 1;
          newtokens[i].StrPos += tokens[tokenFrom].StrPos - 1;
        }
        if(newtokens.length) newtokens[0].Pre = tokens[tokenFrom].Pre;

        //Replace old tokens with new tokens
        newtokens.unshift(tokenTo - tokenFrom + 1);
        newtokens.unshift(tokenFrom);
        tokens.splice.apply(tokens, newtokens);

        if(last_token_index >= tokens.length) last_token_index = tokens.length - 1;
        
        rslt.stats.count++;
      }
    } while(found_function_at !== null);
  }
  return rslt;
}

function replaceMacro(sql, Macros, log, context){
  var sqlids = {};
  var rslt = {
    sql: sql,
    stats: {
      count: 0
    }
  };
  for(let sqlid in Macros){
    //Convert all functions to arrays of tokens
    sqlids[sqlid.toUpperCase()] = sqlid;
  }

  if(!sql) return rslt;

  //Replace full string, if applicable
  var uppersql = sql.toString().trim().toUpperCase();
  var idxsemi = uppersql.indexOf(';');
  if(idxsemi>=0) uppersql = uppersql.substr(0,idxsemi).trim();
  if(uppersql in sqlids){
    let sqlfunc = Macros[sqlids[uppersql]];
    if(sqlfunc && !sqlfunc.params && _.isString(sqlfunc)){
      rslt.sql = sqlfunc + ((idxsemi >= 0) ? uppersql.substr(idxsemi) : '');
      rslt.stats.count++;
      return rslt;
    }
  }

  //Replace tokens
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
      let sqlid = sql.substr(lastidx+3,fnameidx-lastidx-3).trim();
      if(sqlids[sqlid.toUpperCase()]){
        //If the Macros[sqlid] is a function - evaluate the function
        let sqlfunc = Macros[sqlids[sqlid.toUpperCase()]];
        if(sqlfunc && sqlfunc.params){
          let endPos = sql.indexOf(')%%%',lastidx);
          if(endPos >= 0){
            let match = sql.substr(lastidx,endPos-lastidx+4);
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
                rslt.sql = '';
                return rslt;
              }
            }
            var sqlfuncrslt = applySQLFunc(sqlfunc, params, origmatch, context);
            sql = sql.substr(0,lastidx) + sqlfuncrslt + sql.substr(lastidx+origmatch.length);
            rslt.stats.count++;
            lastidx += sqlfuncrslt.length;
          }
        }
        else if(typeof sqlfunc != 'undefined'){
          let endPos = sql.indexOf('%%%',lastidx+3);
          if(endPos >= 0){
            let match = sql.substr(lastidx,endPos-lastidx+3);
            sql = sql.substr(0,lastidx) + sqlfunc + sql.substr(lastidx+match.length);
            rslt.stats.count++;
            lastidx += sqlfunc.length;
          }
        }
      }
    }
    if(lastidx <= 0) lastidx = -1;
    else lastidx = sql.lastIndexOf('%%%',lastidx-1);
  }
  rslt.sql = sql;
  return rslt;
}

function replaceSchema(sql, schema_replacement){
  if(!schema_replacement) return sql;

  var schematokens = [];
  for(let i=0;i<schema_replacement.length;i++){
    var schema_replacement_info = schema_replacement[i];
    //Convert all functions to arrays of tokens
    try{ schematokens.push({ 'name': schema_replacement_info.search_schema, 'tokens': SQLScanner.Scan(schema_replacement_info.search_schema), 'replace': schema_replacement_info.replace_schema }); }
    catch(ex) { /* Do nothing */ }
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
    for(let i=tokens.length - 1;i >= 0; i--){
      let ctoken = tokens[i];
      if((ctoken.Name=='ID')||(ctoken.Name=='FUNCTION')){
        for(let j=0; j<schematokens.length; j++){
          schematoken = schematokens[j];
          let match = true;
          for(let k=0;k < (schematoken.tokens.length-1); k++){
            let ft = schematoken.tokens[schematoken.tokens.length-k-2];
            if((i-k-1) < 0) { /* Do nothing */ }
            else if(ft.Name==tokens[i-k-1].Name){
              let fval = (ft.Value||'').toString();
              let tval = (tokens[i-k-1].Value||'').toString();
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
        let ctoken = tokens[i];
        if((ctoken.Name==dbobj.Name)&&(ctoken.Value.toLowerCase()==dbobj.Value.toLowerCase())){
          var numTokens = 1;
          var srcStart = ctoken.StrPos - 1;
          var srcLen = 0;

          let match = true;
          for(let k=0;k < (schematoken.tokens.length-1); k++){
            let ft = schematoken.tokens[schematoken.tokens.length-k-2];
            if((i-k-1) < 0) { /* Do nothing */ }
            else if(ft.Name==tokens[i-k-1].Name){
              let fval = (ft.Value||'').toString();
              let tval = (tokens[i-k-1].Value||'').toString();
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
  if(!options) options = { schema_replacement: [], log: console.log, context: {} }; // eslint-disable-line no-console
  if(!options.context) options.context = {};
  if(!('vars' in options.context)) options.context.vars = {};
  sql = exports.ParseMultiLine(sql);
  if (sqlext){
    if(sqlext.Funcs){
      if(sql in sqlext.Funcs) sql = sqlext.Funcs[sql];

      var newsql = sql;
      
      do{
        sql = newsql;
        //var startTime = Date.now(); //Performance Timing
        var rsltMacro = replaceMacro(newsql, sqlext.Funcs, options.log, options.context);
        newsql = rsltMacro.sql;
        var rsltFunc = replaceSQLFunc(rsltMacro.sql, sqlext.Funcs, options.context);
        newsql = rsltFunc.sql;
        //console.log('Time: '+(Date.now()-startTime)+' ms    Macro: '+rsltMacro.stats.count+' Func: '+rsltFunc.stats.count); //Performance Timing
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
};
exports.ReplaceAll = function (val, find, replace){
  return val.split(find).join(replace);
};
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
};
exports.escapeRegEx = function (q) {
  return q.replace(/[-[\]{}()*+?.,\\/^$|#\s]/g, '\\$&');
};
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
};
exports.waitDefer = function(f, timeout){
  var lastTimeout = null;
  return function(options){
    if(!options) options = {};
    if(lastTimeout){ clearTimeout(lastTimeout); lastTimeout = null; }
    if(options.clearTimeout){ return; }
    lastTimeout = setTimeout(f, timeout);
  };
};
exports.LogDBResult = function(platform, rslt){ //sql, dbrslt, notices, warnings
  var _LEVEL_WARNING = 2;
  var _LEVEL_INFO = 4;
  var db_log_level = platform.Config.debug_params.db_log_level;
  if(rslt.warnings && (db_log_level & _LEVEL_WARNING)){
    for(let i=0;i<rslt.warnings.length;i++){
      platform.Log.warning('SQL WARNING: '+rslt.warnings[i].message, { source: 'database' });
    }
  }
  if(rslt.notices && (db_log_level & _LEVEL_INFO)){
    for(let i=0;i<rslt.notices.length;i++){
      platform.Log.info('SQL NOTICE: '+rslt.notices[i].message, { source: 'database' });
    }
  }
};
exports.Soundex = function(str){
  var chrs = str.toLowerCase().split('');
  var sounds = {};
  _.each('bfpv', function(chr){ sounds[chr] = '1'; });
  _.each('cgjkqsxz', function(chr){ sounds[chr] = '2'; });
  _.each('dt', function(chr){ sounds[chr] = '3'; });
  _.each('l', function(chr){ sounds[chr] = '4'; });
  _.each('mn', function(chr){ sounds[chr] = '5'; });
  _.each('r', function(chr){ sounds[chr] = '6'; });
  for(var i=0;i<chrs.length;i++){
    if(i < (chrs.length-1)){
      if(sounds[chrs[i]]==sounds[chrs[i+1]]){ chrs.splice(i+1,1); i--; continue; }
    }
    if(i < (chrs.length-2)){
      if((chrs[i+1]=='h')||(chrs[i+1]=='w')){
        if(sounds[chrs[i]]==sounds[chrs[i+2]]){ chrs.splice(i+2,1); i--; continue; }
      }
    }
  }
  var rslt = chrs.shift().toUpperCase();
  rslt += _.map(chrs, function (chr) { return sounds[chr]||''; }).join('');
  return (rslt+'000').substr(0,4);
};
exports.ParseLOVValues = function(jsh, values){
  if(!values) return [];
  if(_.isArray(values)) return values;
  var rslt = [];
  for(var key in values){
    var newval = {};
    newval[jsh.map.code_val] = key;
    newval[jsh.map.code_txt] = values[key];
    rslt.push(newval);
  }
  return rslt;
};