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

var DB = require('../index');
var assert = require('assert');
var _ = require('lodash');

var dbconfig = { _driver: new DB.noDriver() };
var db = new DB(dbconfig);

describe('Basic',function(){
  it('Types', function (done) {
    assert((DB.types.VarChar(50).length==50),'Success');
    done();
  });
  it('ParseSQL', function (done) {
    var sql = db.ParseSQL([
      'select * from',
      'TEST'
    ]);
    assert((sql=='select * from TEST'),'Success');
    done();
  });
  it('ParseSQL JSH', function (done) {
    db.SQLExt.Funcs['sample'] = 'select * from TEST';
    var sql = db.ParseSQL('sample');
    assert((sql=='select * from TEST'),'Success');
    done();
  });
  it('Util', function (done) {
    assert((DB.util.ReplaceAll('abcdefabc','abc','def')=='defdefdef'),'Success');
    done();
  });
  it('Log', function (done) {
    var did_log = false;
    db.platform.Log = function(msg){ did_log = true; };
    db.platform.Log.info = function(msg){ db.platform.Log(); };
    db.platform.Log.warning = function(msg){ db.platform.Log(); };
    db.platform.Log.error = function(msg){ db.platform.Log(); };
    db.Log('Test Log');
    assert(did_log,'Success');
    done();
  });
  it('Soundex', function(done){
    var soundex_tests = {
      'A': 'A000',
      'Ab': 'A100',
      'abcd': 'A123',
      'Ac': 'A200',
      'Ajmr': 'A256',
      'Adl': 'A340',
      'Bbcd': 'B230',
      'BCDL': 'B234',
      'CAaEeIiOoUuHhYybcd': 'C123',
      'Dbcdlmr': 'D123',
      'Gbfcgdt': 'G123',
      'I': 'I000',
      'Jbwb': 'J100',
      'Jbob': 'J110',
      'Robert': 'R163',
      'Rupert': 'R163',
      'Rubin': 'R150',
      'Ashcraft': 'A261',
      'Ashcroft': 'A261',
      'Tymczak': 'T522',
      'Pfister': 'P236',
      'Honeyman': 'H555',
    };
    
    _.each(soundex_tests, function(soundex_expected_result, soundex_test){
      var soundex_result = DB.util.Soundex(soundex_test);
      assert(soundex_result == soundex_expected_result);
    });
    done();
    
  });
});