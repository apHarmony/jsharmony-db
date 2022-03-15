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
jsh.map.code_txt = 'code_txt';
jsh.map.code_val = 'code_val';
jsh.map.code_parent = 'code_parent';
jsh.map.code_end_date = 'code_end_date';
jsh.map.code_seq = 'code_val';
jsh.map.code = 'code';
jsh.map.code2 = 'code2';
jsh.map.code_sys = 'code';
jsh.map.code2_sys = 'code2';
jsh.map.code_app = 'code';
jsh.map.code2_app = 'code2';

exports = module.exports = function shouldGenerateFormSql(db, DB, primaryKey, timestampType) {
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

  describe('getModelRecordset', function() {
    it('can call getModelRecordset', function() {
      var model = {
        table: 'sql_test',
      };
      var sql_searchfields = [];
      var allfields = [
        {name: 'name', sqlselect: 'value'},
        {name: 'other'},
      ]
      var sortfields = [];
      var searchfields = [];
      var datalockqueries = [];
      var rowstart = 1;
      var rowcount = 10;
      var dbsql = db.sql.getModelRecordset(jsh, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries, rowstart, rowcount);
      console.log(dbsql);
      assert(dbsql.sql.match(/select/i));
    });

    it('can execute getModelRecordset main sql', function(done) {
      var model = {
        table: 'sql_test',
      };
      var sql_searchfields = [];
      var allfields = [
        {name: 'id'},
        {name: 'name'},
      ]
      var sortfields = [];
      var searchfields = [];
      var datalockqueries = [];
      var rowstart = 1;
      var rowcount = 10;
      var dbsql = db.sql.getModelRecordset(jsh, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries, rowstart, rowcount);
      console.log(dbsql);
      db.Row('', dbsql.sql, [], {}, done);
    });

    it('can execute getModelRecordset count sql', function(done) {
      var model = {
        table: 'sql_test',
      };
      var sql_searchfields = [];
      var allfields = [
        {name: 'id'},
        {name: 'name'},
      ]
      var sortfields = [];
      var searchfields = [];
      var datalockqueries = [];
      var rowstart = 1;
      var rowcount = 10;
      var dbsql = db.sql.getModelRecordset(jsh, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries, rowstart, rowcount);
      console.log(dbsql);
      db.Row('', dbsql.rowcount_sql, [], {}, done);
    });

    it('can execute getModelRecordset with search', function(done) {
      var model = {
        table: 'sql_test',
      };
      var sql_searchfields = [];
      var allfields = [
        {name: 'id'},
        {name: 'name'},
      ]
      var sortfields = [];
      var searchfields = ['name = \'foo\''];
      var datalockqueries = [];
      var rowstart = 1;
      var rowcount = 10;
      var dbsql = db.sql.getModelRecordset(jsh, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries, rowstart, rowcount);
      console.log(dbsql);
      db.Row('', dbsql.sql, [], {}, done);
    });

    // LOV

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
          field: { type: DB.types.Int, sql_to_db: '42' },
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

  describe('postModelExec', function() {
    it('can call postModelExec', function() {
      var model = {
        table: 'sql_test',
        sqlexec: 'select * from sql_test',
      };
      var param_datalocks = [];
      var datalockqueries = [];
      var sql = db.sql.postModelExec(jsh, model, param_datalocks, datalockqueries);
      console.log(sql);
      assert(sql.match(/select/i));
    });

    it('can execute postModelExec', function(done) {
      var model = {
        table: 'sql_test',
        sqlexec: 'select * from sql_test',
      };
      var param_datalocks = [];
      var datalockqueries = [];
      var sql = db.sql.postModelExec(jsh, model, param_datalocks, datalockqueries);
      console.log(sql);
      db.Row('', sql, [], {}, done);
    });
  });

  describe('deleteModelForm', function() {
    it('can call deleteModelForm', function() {
      var model = {
        table: 'sql_test',
      };
      var keys = [
        {name: 'id'},
      ];
      var datalockqueries = [];
      var sql = db.sql.deleteModelForm(jsh, model, keys, datalockqueries);
      console.log(sql);
      assert(sql.match(/delete/i));
    });

    it('can execute deleteModelForm', function(done) {
      var model = {
        table: 'sql_test',
      };
      var keys = [
        {name: 'id'},
      ];
      var datalockqueries = [];
      var sql = db.sql.deleteModelForm(jsh, model, keys, datalockqueries);
      console.log(sql);
      db.Row('', sql, [DB.types.Int], {id: 1}, done);
    });
  });

  describe('multisel', function() {
    before(function(done) {
      if (!timestampType) {
        throw "Please pass a timestampType to shouldGenerateFormSql"
      }
      db.Command('', 'drop table if exists test_flag; create table test_flag (test_id bigint, test_flag_id bigint, test_flag_type varchar(20)); drop table if exists code_test_flag_type; create table code_test_flag_type (code_val varchar(20), code_txt varchar(20),code_end_date '+timestampType+');', [], {}, done);
    });

    after(function(done) {
      db.Command('', 'drop table test_flag; drop table code_test_flag_type;', [], {}, done);
    });

    describe('getModelMultisel', function() {
      it('can call getModelMultisel', function() {
        var model = {
          table: 'test_flag',
        };
        var lovfield = {
          name: 'test_flag_type',
          lov: {
            code: 'test_flag_type',
          }
        };
        var allfields = [
          {name: 'test_id'},
          {name: 'test_flag_type'},
        ]
        var sql_foreignkeyfields = [
          {name: 'test_id'},
        ];
        var datalockqueries = [];
        var lov_datalockqueries = [];
        var param_datalocks = [];
        var sql = db.sql.getModelMultisel(jsh, model, lovfield, allfields, sql_foreignkeyfields, datalockqueries, lov_datalockqueries, param_datalocks);
        console.log(sql);
        assert(sql.match(/select/i));
      });

      it('can execute getModelMultisel', function(done) {
        var model = {
          table: 'test_flag',
        };
        var lovfield = {
          name: 'test_flag_type',
          lov: {
            code: 'test_flag_type'
          }
        };
        var allfields = [
          lovfield,
        ]
        var sql_foreignkeyfields = [
          {name: 'test_id'},
        ];
        var datalockqueries = [];
        var lov_datalockqueries = [];
        var param_datalocks = [];
        var sql = db.sql.getModelMultisel(jsh, model, lovfield, allfields, sql_foreignkeyfields, datalockqueries, lov_datalockqueries, param_datalocks);
        console.log(sql);
        db.Row('', sql, [DB.types.Int], {test_id: 1}, done);
      });

      it('can execute getModelMultisel - with datalock', function(done) {
        var model = {
          table: 'test_flag',
        };
        var lovfield = {
          name: 'test_flag_type',
          lov: {
            code: 'test_flag_type'
          }
        };
        var allfields = [
          lovfield,
        ]
        var sql_foreignkeyfields = [
          {name: 'test_id'},
        ];
        var datalockqueries = [];
        var lov_datalockqueries = [];
        var param_datalocks = [
          {
            field: { type: DB.types.Int, sql_to_db: '1' },
            pname: 'id',
            datalockquery: 'id IN (SELECT id FROM sql_test WHERE id=@datalock_id)',
          }
        ];
        var sql = db.sql.getModelMultisel(jsh, model, lovfield, allfields, sql_foreignkeyfields, datalockqueries, lov_datalockqueries, param_datalocks);
        console.log(sql);
        db.Row('', sql, [DB.types.Int, DB.types.Int], {test_id: 1, datalock_id: 1}, function(err, rslt){
          assert.equal(err&&err.message, 'INVALID ACCESS', 'raised a signal');
          done();
        });
      });

      describe('postModelMultisel', function() {
        it('can call postModelMultisel', function() {
          var model = {
            table: 'test_flag',
          };
          var lovfield = {
            name: 'test_flag_type',
            lov: {
              code: 'test_flag_type',
            }
          };
          var lovvals = [
            null,
            null,
          ];
          var foreignkeyfields = [
            {name: 'test_id'},
          ];
          var param_datalocks = [];
          var datalockqueries = [];
          var lov_datalockqueries = [];
          var sql = db.sql.postModelMultisel(jsh, model, lovfield, lovvals, foreignkeyfields, param_datalocks, datalockqueries, lov_datalockqueries);
          console.log(sql);
          assert(sql.match(/delete/i), 'has a delete');
          assert(sql.match(/insert/i), 'has an insert');
        });

        it('can execute postModelMultisel', function(done) {
          var model = {
            table: 'test_flag',
          };
          var lovfield = {
            name: 'test_flag_type',
            lov: {
              code: 'test_flag_type',
            }
          };
          var lovvals = [
            null,
            null,
          ];
          var foreignkeyfields = [
            {name: 'test_id'},
          ];
          var param_datalocks = [];
          var datalockqueries = [];
          var lov_datalockqueries = [];
          var sql = db.sql.postModelMultisel(jsh, model, lovfield, lovvals, foreignkeyfields, param_datalocks, datalockqueries, lov_datalockqueries);
          console.log(sql);
          db.Row('', sql, [DB.types.Int, DB.types.VarChar(20), DB.types.VarChar(20)], {test_id: 1, multisel0: 'one', multisel1: 'two'}, done);
        });

        it('can execute postModelMultisel - with datalock', function(done) {
          var model = {
            table: 'test_flag',
          };
          var lovfield = {
            name: 'test_flag_type',
            lov: {
              code: 'test_flag_type',
            }
          };
          var lovvals = [
            null,
            null,
          ];
          var foreignkeyfields = [
            {name: 'test_id'},
          ];
          var param_datalocks = [
            {
              field: { type: DB.types.Int, sql_to_db: '1' },
              pname: 'id',
              datalockquery: 'id IN (SELECT id FROM sql_test WHERE id=@datalock_id)',
            }
          ];
          var datalockqueries = [];
          var lov_datalockqueries = [];
          var sql = db.sql.postModelMultisel(jsh, model, lovfield, lovvals, foreignkeyfields, param_datalocks, datalockqueries, lov_datalockqueries);
          console.log(sql);
          db.Row('', sql, [DB.types.Int, DB.types.VarChar(20), DB.types.VarChar(20), DB.types.Int], {test_id: 1, multisel0: 'one', multisel1: 'two',datalock_id: 1}, function(err, rslt){
            assert.equal(err&&err.message, 'INVALID ACCESS', 'raised a signal');
            done();
          });
        });
      });  
    });
  });

  describe('LOV', function() {
    before(function(done) {
      if (!timestampType) {
        throw "Please pass a timestampType to shouldGenerateFormSql"
      }
      db.Command('', 'drop table if exists code_test; create table code_test (code_val varchar(20), code_txt varchar(20), code_end_date '+timestampType+'); drop table if exists code2_test; create table code2_test (code_val1 varchar(20), code_val2 varchar(20), code_txt varchar(20), code_end_date '+timestampType+');', [], {}, done);
    });
  
    after(function(done) {
      db.Command('', 'drop table code_test; drop table code2_test;', [], {}, done);
    });

    describe('getLOV', function() {
      it('can execute getLOV - noop', function(done) {
        var fname = 'id';
        var lov = {};
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - sql', function(done) {
        var fname = 'id';
        var lov = {
          sql: 'SELECT * FROM code_test;'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - sql2', function(done) {
        var fname = 'id';
        var lov = {
          sql2: 'SELECT * FROM code2_test;'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - sqlmp', function(done) {
        var fname = 'id';
        var lov = {
          sqlmp: 'SELECT * FROM code2_test;'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code', function(done) {
        var fname = 'id';
        var lov = {
          code: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code2', function(done) {
        var fname = 'id';
        var lov = {
          code2: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code_sys', function(done) {
        var fname = 'id';
        var lov = {
          code_sys: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code2_sys', function(done) {
        var fname = 'id';
        var lov = {
          code2_sys: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code_app', function(done) {
        var fname = 'id';
        var lov = {
          code_app: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - code2_app', function(done) {
        var fname = 'id';
        var lov = {
          code2_app: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOV - with datalock', function(done) {
        var fname = 'id';
        var lov = {
          code: 'test'
        };
        var datalockqueries = [];
        var param_datalocks = [
          {
            field: { type: DB.types.Int, sql_to_db: '1' },
            pname: 'id',
            datalockquery: 'id IN (SELECT id FROM sql_test WHERE id=@datalock_id)',
          }
        ];
        var options = {};
        var sql = db.sql.getLOV(jsh, fname, lov, datalockqueries, param_datalocks, options);
        console.log(sql);
        db.Row('', sql, [DB.types.Int], {datalock_id: 1}, function(err, rslt){
          assert.equal(err&&err.message, 'INVALID ACCESS', 'raised a signal');
          done();
        });
      });
    });

    describe('getLOVFieldTxt', function() {
      it('can execute getLOVFieldTxt - noop', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'id',
          lov: {},
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - values', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'id',
          sqlselect: "'ACTIVE'",
          lov: {
            values: [
              { "code_val": "ACTIVE", "code_txt": "Active" },
              { "code_val": "CLOSED", "code_txt": "Closed" },
            ]
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - values parent', function(done) {
        var model = {
          table: 'sql_test',
          "fields":[
            {
              "name":"Make",
              "sqlselect": "'FORD'",
              "lov":{
                "values":[
                  { "code_val": "FORD", "code_txt": "Ford" },
                  { "code_val": "TOYOTA", "code_txt": "Toyota" }
                ]
              }
            },
            {
              "name":"Model",
              "sqlselect":"'FORD_F150'",
              "lov":{
                "parent":"Make",
                "values":[
                  { "code_val1": "FORD", "code_val2": "FORD_F150", "code_txt": "F150" },
                  { "code_val1": "FORD", "code_val2": "FORD_MUSTANG", "code_txt": "Mustang" },
                  { "code_val1": "TOYOTA", "code_val2": "TOYOTA_COROLLA", "code_txt": "Corolla" },
                  { "code_val1": "TOYOTA", "code_val2": "TOYOTA_CAMRY", "code_txt": "Camry" },
                  { "code_val1": "TOYOTA", "code_val2": "TOYOTA_AVALON", "code_txt": "Avalon" }
                ]
              }
            }
          ]
        };
        var field = model.fields[1];
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - sqlselect', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'id',
          sqlselect: "'ACTIVE'",
          lov: {
            sqlselect: 'SELECT ID "code_val", NAME "code_txt" FROM SQL_TEST'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            code: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code2', function(done) {
        var model = {
          table: 'sql_test',
          fields: [
            {
              name: 'other',
              sqlselect: "'other'",
            }
          ]
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            parent: 'other',
            code2: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code_sys', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            code_sys: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code2_sys', function(done) {
        var model = {
          table: 'sql_test',
          fields: [
            {
              name: 'other',
              sqlselect: "'other'",
            }
          ]
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            parent: 'other',
            code2_sys: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code_app', function(done) {
        var model = {
          table: 'sql_test',
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            code_app: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });

      it('can execute getLOVFieldTxt - code2_app', function(done) {
        var model = {
          table: 'sql_test',
          fields: [
            {
              name: 'other',
              sqlselect: "'other'",
            }
          ]
        };
        var field = {
          name: 'name',
          sqlselect: "'ACTIVE'",
          lov: {
            parent: 'other',
            code2_app: 'test'
          }
        };
        var sql = db.sql.getLOVFieldTxt(jsh, model, field);
        console.log(sql);
        db.Row('', sql, [], {}, done);
      });
    });
  });
}