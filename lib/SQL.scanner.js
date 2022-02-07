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

var SQLToken = require('./SQL.token.js');

exports = module.exports = {};

//Scans JSON string and returns array of tokens
exports.Scan = function(str, filename) {
  var tokens = [];
  if (!str) return tokens;
  var input = { Pos: 0, Line: 1, File: filename, StartPos: 0, StartLine: 1, StartPre: '', StrPos: 0 };
  var strpos = 0;
  var strlen = str.length;
  var ws = '';
  
  function GetNextChar(no_newline) {
    if (strpos >= strlen) return '';
    var rslt = str[strpos];
    if(rslt=='%'){
      var jsexec_escape_start = '%%%JSEXEC_ESCAPE(';
      var jsexec_escape_end = ')%%%';
      if((strlen-strpos+1)>=(jsexec_escape_start.length+jsexec_escape_end.length)){
        if(str.substr(strpos,jsexec_escape_start.length)==jsexec_escape_start){
          //strpos += jsexec_escape_start.length;
          var endidx = str.indexOf(jsexec_escape_end,strpos);
          if(endidx >= 0){
            strpos = endidx + jsexec_escape_end.length;
            return GetNextChar(no_newline);
          }
        }
      }
    }
    if (no_newline && ((rslt == '\r') || (rslt == '\n'))) return '';
    input.Pos++;
    strpos++;
    return rslt;
  }
  
  function PeekNextChar() {
    if (strpos >= strlen) return '';
    var rslt = str[strpos];
    return rslt;
  }
  
  while (strpos < strlen) {
    var c = GetNextChar();
    var val = '';
    input.StartLine = input.Line;
    input.StartPos = input.Pos;
    input.StrPos = strpos;
    
    //White Space
    if (IsWhiteSpace(c)) {
      //if (c == '\t') ws += '  '; //Convert tabs to two-spaces
      //else ws += c;
      ws += c;
      if (c == '\n') { input.Line++; input.Pos = 0; }
      continue;
    }
    //Comments --
    if ((c == '-') && (PeekNextChar() == '-')) {
      ws += c;
      while ((c != '\n') && (c !== '')) {
        c = GetNextChar();
        ws += c;
      }
      if (c == '\n') { input.Line++; input.Pos = 0; }
      continue;
    }
    //Comments /* */
    if ((c == '/') && (PeekNextChar() == '*')) {
      ws += c;
      c = GetNextChar(); ws += c;
      while (!((c == '*') && (PeekNextChar() == '/')) && (c !== '')) {
        c = GetNextChar();
        ws += c;
        if (c == '\n') { input.Line++; input.Pos = 0; }
      }
      if(c !== ''){c = GetNextChar(); ws += c; }
      continue;
    }
    
    input.StartPre = ws;
    ws = '';
    
    //'' String, or "" ID, `` ID
    if ((c == '"')||(c == "'")||(c == '`')) {
      var quotchar = c;
      val = '';
      c = '';
      var isMultiline = false;
      do {
        if((c == quotchar) && (PeekNextChar() == quotchar)){
          c = GetNextChar();
        }
        val += c;
        c = GetNextChar();
        if (c == '\n') { isMultiline = true; input.Line++; input.Pos = 0; }
      }
      while ((strpos < strlen) && (c != quotchar) && (c !== ''));
      //Error, unclosed string
      if ((c != quotchar) || (c === '')) { throw new ScanError(input, 'Unclosed '+quotchar+'string'+quotchar+': '+val); }
      //Add token
      if(quotchar=="'") tokens.push(new SQLToken.STRING(input, val, isMultiline));
      else tokens.push(new SQLToken.ID(input, val));
      continue;
    }
    //[] ID
    if(c=='['){
      val = '';
      c = GetNextChar();
      while((c !== '') && (c != ']')){
        c = GetNextChar();
        val += c;
        if (c == '\n') { input.Line++; input.Pos = 0; }
      }
      if (c === '') { throw new ScanError(input, 'Unclosed [ID]'); }
      tokens.push(new SQLToken.ID(input, val));
      continue;
    }
    
    //Hex String
    if ((c=='0') && ((PeekNextChar()=='x') || (PeekNextChar()=='X'))) {
      val = '';
      c = GetNextChar();
      c = '';
      do {
        val += c;
        c = GetNextChar();
      }
      while(IsHexDigit(c));
      //Backtrack
      if(c !== ''){ input.Pos--; strpos--; }
      if(!val) val = '0';
      tokens.push(new SQLToken.NUMBER(input, val, false, true));
      continue;
    }

    //Number
    var isScientific = false;
    if (IsDigit(c) || (c == '-') || ((c == '.') && IsDigit(PeekNextChar()))) {
      isScientific = false;
      val = '';
      if(c == '.') val = '0';
      do {
        val += c;
        c = GetNextChar();
      }
      while(IsDigit(c));
      if (val.substr(0, 2) == '-0' && val.length > 2) { throw new ScanError(input, 'Invalid Number Format'); }
      if (c == '.') {
        do {
          val += c;
          c = GetNextChar();
        }
        while(IsDigit(c));
      }
      if ((c == 'e') || (c == 'E')) {
        isScientific = true;
        val += c;
        c = GetNextChar();
        if ((c == '-') || (c == '+') || IsDigit(c)) {
          do {
            val += c;
            c = GetNextChar();
          }
          while(IsDigit(c));
        }
        else throw new ScanError(input, 'Invalid Mantissa');
      }
      tokens.push(new SQLToken.NUMBER(input, val, isScientific));
      //Backtrack
      if(c !== ''){ input.Pos--; strpos--; }
      continue;
    }

    
    if (c == '(') tokens.push(new SQLToken.LPAREN(input));
    else if (c == ')') tokens.push(new SQLToken.RPAREN(input));
    else if (c == ',') tokens.push(new SQLToken.COMMA(input));
    else if (c == '.') tokens.push(new SQLToken.DOT(input));

    else if (c == '+') tokens.push(new SQLToken.PLUS(input));
    else if (c == '-') tokens.push(new SQLToken.MINUS(input));
    else if (c == '*') tokens.push(new SQLToken.STAR(input));
    else if (c == '/') tokens.push(new SQLToken.FSLASH(input));

    else if (c == '%') tokens.push(new SQLToken.PERCENT(input));
    else if (c == '<'){
      if(PeekNextChar()=='<'){ c = GetNextChar(); tokens.push(new SQLToken.LSHIFT(input)); continue; }
      if(PeekNextChar()=='='){ c = GetNextChar(); tokens.push(new SQLToken.LTE(input)); continue; }
      if(PeekNextChar()=='>'){ c = GetNextChar(); tokens.push(new SQLToken.NEQ(input)); continue; }
      tokens.push(new SQLToken.LT(input));
    }
    else if (c == '>'){
      if(PeekNextChar()=='>'){ c = GetNextChar(); tokens.push(new SQLToken.RSHIFT(input)); continue; }
      if(PeekNextChar()=='='){ c = GetNextChar(); tokens.push(new SQLToken.GTE(input)); continue; }
      tokens.push(new SQLToken.GT(input));
    }
    else if (c == '='){
      if(PeekNextChar()=='='){ c = GetNextChar(); tokens.push(new SQLToken.EQEQ(input)); continue; }
      tokens.push(new SQLToken.EQ(input));
    }

    else if (c == '&') tokens.push(new SQLToken.AMP(input));
    else if (c == '|'){
      if(PeekNextChar()=='|'){ c = GetNextChar(); tokens.push(new SQLToken.CONCAT(input)); continue; }
      tokens.push(new SQLToken.PIPE(input));
    }
    else if (c == '!'){
      if(PeekNextChar()=='='){ c = GetNextChar(); tokens.push(new SQLToken.NEQ(input)); continue; }
      tokens.push(new SQLToken.NOT(input));
    }
    else if (c == '@') tokens.push(new SQLToken.AT(input));

    else if (c == '$') tokens.push(new SQLToken.DOLLAR(input));
    else if (c == '?') tokens.push(new SQLToken.QUESTION(input));
    else if (c == ':') tokens.push(new SQLToken.COLON(input));
    else if (c == '~') tokens.push(new SQLToken.TILDE(input));

    else if (c == '#') tokens.push(new SQLToken.HASH(input));
    else if (c == ';') tokens.push(new SQLToken.SEMICOLON(input));

    else if (IsID(c)) {
      val = c;
      while(IsID(PeekNextChar())){
        c = GetNextChar();
        val += c;
      }
      tokens.push(new SQLToken.ID(input, val));
      continue;
    }
    else { throw new ScanError(input, 'Invalid Token: ' + c); }
  }
  tokens.push(new SQLToken.END(input, ws));
  return tokens;
};

function IsID(c) {
  if(IsWhiteSpace(c)) return false;
  if (
    (c == '(') || (c == ')') || (c == ',') || (c == '.') ||
    (c == '+') || (c == '-') || (c == '*') || (c == '/') ||
    (c == '%') || (c == '<') || (c == '>') || (c == '=') ||
    (c == '&') || (c == '|') || (c == '!') || (c == '@') ||
    (c == '$') || (c == '?') || (c == ':') || (c == '~') ||
    (c == '#') || (c == ';') || (c == '')
  ) return false;
  return true;
}

function IsDigit(c, dot) {
  if (
    (c == '1') || (c == '2') || (c == '3') || (c == '4') ||
    (c == '5') || (c == '6') || (c == '7') || (c == '8') ||
    (c == '9') || (c == '0')
  ) return true;
  if (dot && (c == '.')) return true;
  return false;
}

function IsHexDigit(c, dot) {
  if (
    (c == '1') || (c == '2') || (c == '3') || (c == '4') ||
    (c == '5') || (c == '6') || (c == '7') || (c == '8') ||
    (c == '9') || (c == '0') || (c == 'A') || (c == 'B') ||
    (c == 'C') || (c == 'D') || (c == 'E') || (c == 'F') ||
    (c == 'a') || (c == 'b') || (c == 'c') || (c == 'd') ||
    (c == 'e') || (c == 'f')
  ) return true;
  return false;
}

function IsWhiteSpace(c) {
  return ((c == ' ') || (c == '\t') || (c == '\r') || (c == '\n') || (c == '\f'));
}

function ScanError(input, msg) {
  this.name = 'ScanError';
  this.file = input.File;
  this.message = msg;
  if (!this.message) this.message = 'Error Parsing';
  this.startpos = { line: input.StartLine, char: input.StartPos };
  this.endpos = { line: input.Line, char: input.Pos };
  this.stack = (new Error()).stack;
}
ScanError.prototype = Object.create(Error.prototype);
ScanError.prototype.constructor = ScanError;
