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
var path = require('path');
var fs = require('fs');

var objects = [
  {
    "name": "test.document",
    "path": path.join(__dirname, "/document.json"),
    "moduleName": "test",
    "type": "table",
    "caption": ["Table","Tables"],
    "columns": [
      { "name": "id", "type": "bigint", "key": true, "identity": true, "null": false },
      { "name": "name", "type": "varchar", "length": 256, "null": false, "unique": true },
      { "name": "etstmp", "type": "datetime", "length": 7, "null": true, "default": { "sql": "%%%jsh.map.timestamp%%%" } },
      { "name": "euser", "type": "varchar", "length": 20, "null": true, "default": { "sql": "%%%jsh.map.current_user%%%" } },
      { "name": "mtstmp", "type": "datetime", "length": 7, "null": true },
      { "name": "muser", "type": "varchar", "length": 20, "null": true }
    ],
    "triggers": [
      { "on": ["update", "insert"], "exec": [
          "set(mtstmp,%%%jsh.map.timestamp%%%);",
          "set(muser,%%%jsh.map.current_user%%%);"
        ]
      }
    ],
    "sample_data": [
      { "name": "Default Value", "_FILES": { "a_file.txt": "{{id}}.txt" } }
    ],
    "unique": [
      ["id", "name"]
    ],
    "index": [
      { "columns": ["id", "name"] },
      { "columns": ["name"] }
    ],
    "_foreignkeys": {},
    "_dependencies": {}
  },
  {
    "name": "test.multi",
    "path": path.join(__dirname, "/document.json"),
    "moduleName": "test",
    "type": "table",
    "caption": ["Multi","Multis"],
    "columns": [
      { "name": "id", "type": "bigint", "key": true, "null": false },
      { "name": "beta", "type": "bigint", "key": true, "null": false },
    ],
    "_foreignkeys": {},
    "_dependencies": {}
  },
  {
    "name": "test.v_document",
    "path": "./dir/document.json",
    "moduleName": "test",
    "type": "view",
    "caption": ["View","Views"],
    "tables": {
      "test.document": {
        "columns": [
          { "name": "id", "type": "bigint", "sqlselect": "min(id)"},
          { "name": "name", "type": "varchar", "length": 256, "sqlselect": "min(name)"}
        ]
      }
    },
    "where": "document.name != 'Default Value'",
    "triggers": [
      {"on": ["insert"], "exec": [
          "insert into {schema}.document(name) values(inserted(name))",
          "increment_changes()"
        ]
      },
      {"on": ["update"], "exec": [
          "update {schema}.document set name=inserted(name) where id = deleted(id)",
          "increment_changes()"
        ]
      },
      {"on": ["delete"], "exec": [
          "delete from {schema}.document where id = deleted(id);",
          "increment_changes()"
        ]
      }
    ],
    "_tables": {}
  },
  {
    "name": "test.signatory",
    "path": "./dir/host.json",
    "moduleName": "test",
    "type": "table",
    "caption": ["Signatory","Signatories"],
    "columns": [
      { "name": "id", "type": "bigint", "key": true, "identity": true, "null": false },
      { "name": "x", "type": "bigint" },
    ],
    "triggers": [
      { "on": ["insert"], "exec": [
          "insert into {schema}.other(x) values(0)",
          "insert into {schema}.other(x) values(0)"
        ]
      }
    ],
    "_foreignkeys": {},
    "_dependencies": {}
  },
  {
    "name": "test.witness",
    "path": "./dir/host.json",
    "moduleName": "test",
    "type": "table",
    "caption": ["Witness","Witnesses"],
    "columns": [
      { "name": "id", "type": "bigint", "key": true, "identity": true, "null": false },
      { "name": "reported_insert_id", "type": "bigint" },
      { "name": "reported_last_insert_identity", "type": "bigint" },
    ],
    "sample_data": [
      { "reported_insert_id": 0, "reported_last_insert_identity": 0 }
    ],
    "_foreignkeys": {},
    "_dependencies": {}
  },
  {
    "name": "test.other",
    "path": "./dir/host.json",
    "moduleName": "test",
    "type": "table",
    "caption": ["Other","Others"],
    "columns": [
      { "name": "id", "type": "bigint", "key": true, "identity": true, "null": false },
      { "name": "x", "type": "bigint" },
    ],
    "_foreignkeys": {},
    "_dependencies": {}
  },
  {
    "name": "test.v_host",
    "path": "./dir/host.json",
    "moduleName": "test",
    "type": "view",
    "caption": ["Host","Hosts"],
    "tables": {
      "test.signatory": {
        "columns": [
          { "name": "id"},
          { "name": "x"}
        ]
      }
    },
    "triggers": [
      {"on": ["insert"], "exec": [[
          "with_insert_identity(signatory, id, ",
          "  insert into {schema}.signatory(x) values(0),",
          "  return_insert_key(signatory, id, (id=@@INSERT_ID));",
          "  update {schema}.witness set reported_insert_id=@@INSERT_ID, reported_last_insert_identity=(select max(id) from signatory)",
          ")"
        ]]
      }
    ],
    "_tables": {}
  },
];

exports = module.exports = function shouldBehaveLikeAnObject(db, DB, timestampIn, timestampOut, rowcountSql) {
  var options = {
    dbconfig: db.dbconfig,
    sqlFuncs: {
      "$ifnull":{ "params": ["a","b"], "sql": "ifnull(%%%a%%%,%%%b%%%)" },
    },
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
    var testDataPath = path.join(__dirname, '..', 'data');
    if(!fs.existsSync(testDataPath)) fs.mkdirSync(testDataPath, '0777');
    db.platform.Config.datadir = testDataPath;
    db.SQLExt.Funcs['jsh.map.timestamp'] = timestampIn;
    db.SQLExt.Funcs['jsh.map.current_user'] = "'user'";
    db.SQLExt.Objects['test'] = objects;
    db.RunScriptArray(db.platform, [
      {sql:'object:drop', module: 'test'},
      {sql:'object:init', module: 'test'},
      {sql:'object:restructure_init', module: 'test'},
      {sql:'object:init_data', module: 'test'},
      {sql:'object:sample_data', module: 'test'},
    ], options, done);
  });

  after(function(done) {
    db.RunScriptArray(db.platform, [
      {sql:'object:drop', module: 'test'},
    ], options, function() {
      db.Close(done);
    });
  });

  it('should be executed', function() {
    assert.ok(db);
  });

  it('should create the table', function(done) {
    db.Scalar('','select count(*) from test.document',[],{},function(err,rslt){
      if(err) return done(err);
      assert.equal(rslt, 1);
      return done();
    });
  });

  it('should include sample data', function(done) {
    db.Scalar('','select name from test.document',[],{},function(err,rslt){
      if(err) return done(err);
      assert.equal(rslt, 'Default Value');
      return done();
    });
  });

  it('should set default timestamp', function(done) {
    db.Scalar('','select etstmp from test.document',[],{},function(err,rslt){
      if(err) console.log(err);
      assert.equal(rslt, timestampOut);
      return done();
    });
  });

  it('should set modified timestamp', function(done) {
    db.Scalar('','select mtstmp from test.document',[],{},function(err,rslt){
      if(err) return done(err);
      assert.equal(rslt, timestampOut);
      return done();
    });
  });

  it('supports multiple keys', function(done) {
    db.Command('','insert into test.multi(id, beta) values (1, 1)',[],{},function(err,rslt){
      if(err) return done(err);
      assert(!err);
      return done();
    });
  });

  it('view exists', function(done) {
    db.Scalar('','select count(*) from test.v_document',[],{},function(err,rslt){
      if(err) return done(err);
      assert.equal(rslt, 1);
      return done();
    });
  });

  it('last insert identity', function(done) {
    db.Command('','insert into test.v_host(x) values (0)',[],{},function(err,rslt){
      if(err) return done(err);
      db.Row('','select reported_insert_id, reported_last_insert_identity from test.witness',[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt[0], rslt[1]);
        return done();
      });
    });
  });

  it('putModelForm returns the new id for a view', function(done) {
    var ent = {};
    var model = {
      table: 'test.v_host',
      _dbdef: {
        table_type: 'view',
        instead_of_insert: true, // mssql
      },
    };
    var fields = [
      {name: 'x'},
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
    db.Row('', dbsql.sql, [DB.types.BigInt], {x: 0}, function(err, rslt) {
      if(err) return done(err);
      assert.ok(rslt && rslt.id);
      done(err);
    });
  });

  describe('updatedable view', function() {
    beforeEach(function(done) {
      db.Command('','insert into test.document(name) values (\'fixture\')',[],{},function(err,rslt){
        return done(err);
      });
    });
    afterEach(function(done) {
      db.Command('','delete from test.document where name != \'Default Value\'',[],{},function(err,rslt){
        return done(err);
      });
    });

    it('view is insertable', function(done) {
      db.Command('','insert into test.v_document(name) values (\'view inserted\')',[],{},function(err,rslt){
        if(err) return done(err);
        db.Scalar('','select count(*) from test.document where name = \'view inserted\'',[],{},function(err,rslt){
          if(err) return done(err);
          assert.equal(rslt, 1);
          return done();
        });
      });
    });

    it('view insert rowcount with changes', function(done) {
      db.Row('',rowcountSql('insert into test.v_document(name) values (\'view inserted\')'),[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt.xrowcount, 1)
        return done();
      });
    });

    it('view insert rowcount without changes', function(done) {
      db.Row('',rowcountSql('insert into test.v_document(name) select (\'view inserted\') where 1=0'),[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt.xrowcount, 0)
        return done();
      });
    });


    it('view is updateable', function(done) {
      db.Command('','update test.v_document set name = \'updated\' where name = \'fixture\'',[],{},function(err,rslt){
        if(err) return done(err);
        db.Scalar('','select count(*) from test.document where name = \'updated\'',[],{},function(err,rslt){
          if(err) return done(err);
          assert.equal(rslt, 1);
          return done();
        });
      });
    });

    it('view update rowcount with changes', function(done) {
      db.Row('',rowcountSql('update test.v_document set name = \'updated\' where name = \'fixture\''),[],{},function(err,rslt){
        if(err) console.log(err);
        assert.equal(rslt.xrowcount, 1)
        return done();
      });
    });

    it('view update rowcount without changes', function(done) {
      db.Row('',rowcountSql('update test.v_document set name = \'updated\' where 1=0'),[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt.xrowcount, 0)
        return done();
      });
    });

    it('view is deleteable', function(done) {
      db.Command('','delete from test.v_document where name = \'fixture\'',[],{},function(err,rslt){
        if(err) console.log(err);
        db.Scalar('','select count(*) from test.document where name = \'fixture\'',[],{},function(err,rslt){
          if(err) return done(err);
          assert.equal(rslt, 0);
          return done();
        });
      });
    });

    it('view delete rowcount with changes', function(done) {
      db.Row('',rowcountSql('delete from test.v_document where name = \'fixture\''),[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt.xrowcount, 1)
        return done();
      });
    });

    it('view delete rowcount without changes', function(done) {
      db.Row('',rowcountSql('delete from test.v_document where 1=0'),[],{},function(err,rslt){
        if(err) return done(err);
        assert.equal(rslt.xrowcount, 0)
        return done();
      });
    });
  });
}