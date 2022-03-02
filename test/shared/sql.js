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
var jsHarmony = require('jsharmony');

var jsh = new jsHarmony({});

exports = module.exports = function shouldGenerateFormSql(db, DB, primaryKey) {
  //console.log(db, DB);
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


  describe('getModelForm', function() {
    it('can call getModelForm', function() {
      var model = {
        table: 'sql_test',
      };
      var selecttype = 'unknown';
      var allfields = [
        {name: 'name', sqlselect: 'value'},
        {name: 'other'},
      ]
      var sql_allkeyfields = [
        {name: 'id'},
      ];
      var datalockqueries = [];
      var sortfields = [];
      var sql = db.sql.getModelForm(jsh, model, selecttype, allfields, sql_allkeyfields, datalockqueries, sortfields);
      console.log(sql);
      assert(sql.match(/select/i));
    });

    it('can execute getModelForm', function(done) {
      var model = {
        table: 'sql_test',
      };
      var selecttype = 'unknown';
      var allfields = [
        {name: 'name'},
      ]
      var sql_allkeyfields = [
        {name: 'id'},
      ];
      var datalockqueries = [];
      var sortfields = [];
      var sql = db.sql.getModelForm(jsh, model, selecttype, allfields, sql_allkeyfields, datalockqueries, sortfields);
      db.Row('', sql, [DB.types.Int], {id: 1}, done);
    });

    it('can execute getModelForm - multiple', function(done) {
      var model = {
        table: 'sql_test',
      };
      var selecttype = 'multiple';
      var allfields = [
        {name: 'name'},
      ]
      var sql_allkeyfields = [];
      var datalockqueries = [];
      var sortfields = [
        {field: 'name', dir: 'asc'}
      ];
      var sql = db.sql.getModelForm(jsh, model, selecttype, allfields, sql_allkeyfields, datalockqueries, sortfields);
      db.Row('', sql, [DB.types.Int], {id: 1}, done);
    });

    // LOV
    // jsharmony:models
  });

  describe('getDefaultTasks', function() {
    it('can call getDefaultTasks', function() {
      var dflt_sql_fields = [
        {
          field: {name: 'name'},
          sql: '\'value\'',
        }
      ];
      var sql = db.sql.getDefaultTasks(jsh, dflt_sql_fields);
      console.log(sql);
      assert(sql.match(/select/i));
    });

    it('can execute getDefaultTasks', function(done) {
      var dflt_sql_fields = [
        {
          field: {name: 'name'},
          sql: '\'value\'',
        }
      ];
      var sql = db.sql.getDefaultTasks(jsh, dflt_sql_fields);
      console.log(sql);
      db.Row('', sql, [], {}, done);
    });
  });

  describe('putModelForm', function() {
    it('can call putModelForm', function() {
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
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      assert(dbsql.sql.match(/insert/i));
    });

    it('can execute putModelForm', function(done) {
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
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      db.Row('', dbsql.sql, [DB.types.VarChar(20)], {name: 'name'}, done);
    });

    it('returns the new id', function(done) {
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
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      db.Row('', dbsql.sql, [DB.types.VarChar(20)], {name: 'name'}, function(err, rslt) {
        console.log(err, rslt);
        assert.ok(rslt && rslt.id);
        done(err);
      });
    });

    it('uses sqlgetinsertkeys', function() {
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
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
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

    it('uses datalocks', function() {
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
      var param_datalocks = [
        {
          field: { type: DB.types.Int, sql_to_db: '1' },
          pname: 'id',
          datalockquery: 'id IN (SELECT id FROM sql_test WHERE id=@datalock_id',
        }
      ];
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      assert(dbsql.sql.match('datalock_id'));
    });

    it('can execute with datalocks', function(done) {
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
      var param_datalocks = [
        {
          field: { type: DB.types.Int, sql_to_db: '1' },
          pname: 'id',
          datalockquery: 'id IN (SELECT id FROM sql_test WHERE id=@datalock_id)',
        }
      ];
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      db.Row('', dbsql.sql, [DB.types.VarChar(20), DB.types.Int], {name: 'name', datalock_id: 1}, function(err, rslt){
        assert.equal(err&&err.message, 'INVALID ACCESS', 'raised a signal');
        done();
      });
    });

    it('uses encrypted fields', function() {
      var model = {
        table: 'sql_test',
      };
      var fields = [
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var encryptedfields = [
        {name: 'name'},
      ];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      assert(dbsql.enc_sql.match('name'));
    });

    it('uses hash fields', function() {
      var model = {
        table: 'sql_test',
      };
      var fields = [
      ];
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var encryptedfields = [];
      var hashfields = [
        {name: 'name'},
      ];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      assert(dbsql.enc_sql.match('name'));
    });

    it('executes encrypted fields', function(done) {
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
      var encryptedfields = [
        {name: 'name'},
      ];
      var hashfields = [];
      var enc_datalockqueries = [];
      var param_datalocks = [];
      var dbsql = db.sql.putModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks);
      console.log(dbsql);
      db.Row('', dbsql.enc_sql, [DB.types.Int, DB.types.VarChar(20)], {id: 1, name: 'name'}, done);
    });
  });

  describe('postModelForm', function() {
    it('can call postModelForm', function() {
      var model = {
        table: 'sql_test',
      };
      var fields = [
        {name: 'name', sqlselect: 'value'},
        {name: 'other'},
      ]
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var hashfields = [];
      var param_datalocks = [];
      var datalockqueries = [];
      var sql = db.sql.postModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, hashfields, param_datalocks, datalockqueries);
      console.log(sql);
      assert(sql.match(/update/i));
    });

    it('can execute postModelForm', function(done) {
      var model = {
        table: 'sql_test',
      };
      var fields = [
        {name: 'name'},
      ]
      var keys = [
        {name: 'id'},
      ];
      var sql_extfields = [];
      var sql_extvalues = [];
      var hashfields = [];
      var param_datalocks = [];
      var datalockqueries = [];
      var sql = db.sql.postModelForm(jsh, model, fields, keys, sql_extfields, sql_extvalues, hashfields, param_datalocks, datalockqueries);
      console.log(sql);
      db.Row('', sql, [DB.types.Int, DB.types.VarChar(20)], {id: 1, name: 'name'}, done);
    });
  });
}