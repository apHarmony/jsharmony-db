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

function TokenBase(_Name, input) {
  this.Name = _Name;
  this.Pos = input.Pos;
  this.Line = input.Line;
  this.Pre = input.StartPre;
  this.StartPos = input.StartPos;
  this.StartLine = input.StartLine;
  this.StrPos = input.StrPos;
}

exports.RPAREN = function(input) { TokenBase.call(this, 'RPAREN', input); };
exports.LPAREN = function(input) { TokenBase.call(this, 'LPAREN', input); };
exports.COMMA = function(input) { TokenBase.call(this, 'COMMA', input); };
exports.DOT = function(input) { TokenBase.call(this, 'DOT', input); };

exports.PLUS = function(input) { TokenBase.call(this, 'PLUS', input); };
exports.MINUS = function(input) { TokenBase.call(this, 'MINUS', input); };
exports.STAR = function(input) { TokenBase.call(this, 'STAR', input); };
exports.FSLASH = function(input) { TokenBase.call(this, 'FSLASH', input); };

exports.PERCENT = function(input) { TokenBase.call(this, 'PERCENT', input); };
exports.LT = function(input) { TokenBase.call(this, 'LT', input); };
exports.GT = function(input) { TokenBase.call(this, 'GT', input); };
exports.EQ = function(input) { TokenBase.call(this, 'EQ', input); };

exports.AMP = function(input) { TokenBase.call(this, 'AMP', input); };
exports.PIPE = function(input) { TokenBase.call(this, 'PIPE', input); };
exports.NOT = function(input) { TokenBase.call(this, 'NOT', input); };
exports.AT = function(input) { TokenBase.call(this, 'AT', input); };

exports.DOLLAR = function(input) { TokenBase.call(this, 'DOLLAR', input); };
exports.QUESTION = function(input) { TokenBase.call(this, 'QUESTION', input); };
exports.COLON = function(input) { TokenBase.call(this, 'COLON', input); };
exports.TILDE = function(input) { TokenBase.call(this, 'TILDE', input); };

exports.HASH = function(input) { TokenBase.call(this, 'HASH', input); };
exports.SEMICOLON = function(input) { TokenBase.call(this, 'SEMICOLON', input); };

exports.RSHIFT = function(input) { TokenBase.call(this, 'RSHIFT', input); };
exports.LSHIFT = function(input) { TokenBase.call(this, 'LSHIFT', input); };
exports.CONCAT = function(input) { TokenBase.call(this, 'CONCAT', input); };
exports.LTE = function(input) { TokenBase.call(this, 'LTE', input); };
exports.GTE = function(input) { TokenBase.call(this, 'GTE', input); };
exports.EQEQ = function(input) { TokenBase.call(this, 'EQEQ', input); };
exports.NEQ = function(input) { TokenBase.call(this, 'NEQ', input); };

exports.END = function (input, _post) { TokenBase.call(this, 'END', input); this.Post = _post; };

exports.STRING = function (input, _Value, _Multiline) {
  TokenBase.call(this, 'STRING', input);
  this.Value = _Value;
  this.Multiline = _Multiline;
};

exports.ID = function (input, _Value) {
  TokenBase.call(this, 'ID', input);
  this.Value = _Value;
};

exports.BLOB = function (input, _Value) {
  TokenBase.call(this, 'ID', input);
  this.Value = _Value;
};

exports.NUMBER = function(input, _Value, _Scientific, _Hex) {
  TokenBase.call(this, 'NUMBER', input);
  this.Value = _Value;
  this.Orig = _Value;
  this.Scientific = _Scientific;
  this.Hex = _Hex;
};

exports.FUNCTION = function(input, _Value) {
  TokenBase.call(this, 'FUNCTION', input);
  this.Value = _Value;
  this.ArgumentPos = [];
  this.EndToken = null;
  this.EndPos = null;
};