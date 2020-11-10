/*
Copyright 2020 apHarmony

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

var assert = require('assert');
var _ = require('lodash');

exports = module.exports = function shouldGenerateFormSql(db, DB, primaryKey) {
  var options = {
    dbconfig: db.dbconfig,
  };

  before(function(done) {
    db.platform.Modules = {
      test: {
        schema: 'test',
        namespace: 'test',
        transform: {
          Apply: function(txt, desc) {
            if(!txt) return txt;
            txt = db.util.ReplaceAll(txt,'{namespace}','test');
            txt = db.util.ReplaceAll(txt,'{schema}','test'); 
            return txt;
          }
        },
      }
    };
    db.Command('', 'drop table if exists sql_test; create table sql_test (id '+primaryKey+', name varchar(20));', [], {}, done)
  });

  after(function(done) {
    db.Command('', 'drop table sql_test;', [], {}, function(){ 
      db.Close(done);
    });
  });

  it('should be executed', function() {
    assert.ok(db);
  });

  describe('putModelForm', function() {
    it('can call putModelForm', function() {
      var ent = {};
      var model = {
        table: 'sql_test',
      };
      var fields = [
        {name: 'name', sqlinsert: 'value'},
        {name: 'other'},
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = ['ext'];
      var sql_extvalues = ['extval'];
      var encryptedfields = [];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      assert(dbsql.sql.match('insert'));
    });

    it('can execute putModelForm', function(done) {
      var ent = {};
      var model = {
        table: 'sql_test',
      };
      var fields = [
        {name: 'name'},
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var encryptedfields = [];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      db.Row('', dbsql.sql, [DB.types.VarChar(20)], {name: 'name'}, done);
    });

    it('returns the new id', function(done) {
      var ent = {};
      var model = {
        table: 'sql_test',
      };
      var fields = [
        {name: 'name'},
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var encryptedfields = [];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      db.Row('', dbsql.sql, [DB.types.VarChar(20)], {name: 'name'}, function(err, rslt) {
        assert.ok(rslt && rslt.id);
        done(err);
      });
    });

    it('uses sqlgetinsertkeys', function() {
      var ent = {};
      var model = {
        table: 'sql_test',
        sqlgetinsertkeys: 'select 1',
      };
      var fields = [
        {name: 'name', sqlinsert: 'value'},
        {name: 'other'},
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = ['ext'];
      var sql_extvalues = ['extval'];
      var encryptedfields = [];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      assert(dbsql.sql.match(model.sqlgetinsertkeys));
    });

    it('can execute with sqlgetinsertkeys', function(done) {
      var ent = {};
      var model = {
        table: 'sql_test',
        sqlgetinsertkeys: 'select 1',
      };
      var fields = [
        {name: 'name'},
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var encryptedfields = [];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      db.Row('', dbsql.sql, [DB.types.VarChar(20)], {name: 'name'}, done);
    });
  });
}