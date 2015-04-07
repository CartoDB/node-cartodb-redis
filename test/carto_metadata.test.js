var redis_config = require('./support/config').redis_pool;

var _           = require('underscore')
    , MetaData  = require('../lib/carto_metadata')(redis_config)
    , assert    = require('assert')
;

suite('metadata', function() {

// NOTE: deprecated in 0.2.0
test('test can retrieve database name from header and redis', function(done){
    var req = {headers: {host: 'vizzuality.cartodb.com'}};
    
    MetaData.getDatabase(req, function(err, data){
        assert.equal(data, 'cartodb_test_user_1_db');
        done();
    });
});

test('can retrieve database name for username', function(done){
    MetaData.getUserDBName('vizzuality', function(err, data){
        assert.equal(data, 'cartodb_test_user_1_db');
        done();
    });
});

// NOTE: deprecated in 0.2.0
test('test can retrieve database host from header and redis', function(done){
    var req = {headers: {host: 'vizzuality.cartodb.com'}};
    
    MetaData.getDatabaseHost(req, function(err, data){
        assert.equal(data, 'localhost');
        done();
    });
});


test('can retrieve database host for username', function(done){
    MetaData.getUserDBHost('vizzuality', function(err, data){
        assert.equal(data, 'localhost');
        done();
    });
});

test('can retrieve database password for username', function(done){
    MetaData.getUserDBPass('vizzuality', function(err, data){
        assert.equal(data, 'secret');
        done();
    });
});

// NOTE: deprecated in 0.2.0
test('test can retrieve id from header and redis', function(done){
    var req = {headers: {host: 'vizzuality.cartodb.com'}};

    MetaData.getId(req, function(err, data){
        assert.equal(data, '1');
        done();
    });
});

test('retrieve id for username', function(done){
    MetaData.getUserId('vizzuality', function(err, data){
        assert.equal(data, '1');
        done();
    });
});

test('can retrieve table privacy for public table', function(done){
    MetaData.getTablePrivacy('cartodb_test_user_1_db', 'public', function(err, privacy) {
        assert.ok(!err, err);
        assert.equal(privacy, '1'); // public has privacy=1
        done();
    });
});

    test('can retrieve table privacy for private table', function(done){
        MetaData.getTablePrivacy('cartodb_test_user_1_db', 'private', function(err, privacy) {
            assert.ok(!err, err);
            assert.equal(privacy, '0'); // private has privacy=0
            done();
        });
    });

// NOTE: deprecated in 0.2.0
test('can retrieve table geometry type from request header and params for public table', function(done){
    var req = {headers: {host: 'vizzuality.cartodb.com'}, params: {table: 'public'} };

    MetaData.getGeometryType(req, function(err, geometryType) {
        assert.equal(geometryType, 'geometry');
        done();
    });
});

    // NOTE: deprecated in 0.2.0
    test('can retrieve table geometry type from request header and params for private table', function(done){
        var req = {headers: {host: 'vizzuality.cartodb.com'}, params: {table: 'private'} };
        MetaData.getGeometryType(req, function(err, geometryType) {
            assert.equal(geometryType, 'point');
            done();
        });
    });

test('can retrieve table geometry type from username and tablename for public table', function(done){
    MetaData.getTableGeometryType('cartodb_test_user_1_db', 'public', function(err, geometryType) {
        assert.equal(geometryType, 'geometry');
        done();
    });
});

    test('can retrieve table geometry type from username and tablename for private table', function(done){
        MetaData.getTableGeometryType('cartodb_test_user_1_db', 'private', function(err, geometryType) {
            assert.equal(geometryType, 'point');
            done();
        });
    });

test('can retrieve map key', function(done){
    MetaData.getUserMapKey('vizzuality', function(err, data){
        assert.equal(data, '1234');
        done();
    });
});

test('retrieves sync slaves if they exist', function(done){
    MetaData.getDBSyncSlaves('1.2.3.4', function(err, data){
        assert.deepEqual(data.sort(), ['1.2.3.5','1.2.3.6'].sort());
        done();
    });
});

test('retrieves empty if there are no sync slaves', function(done){
    MetaData.getDBSyncSlaves('2.3.4.5', function(err, data){
        assert.deepEqual(data, []);
        done();
    });
});

test('retrieves async slaves if they exist', function(done){
    MetaData.getDBAsyncSlaves('1.2.3.4', function(err, data){
        assert.deepEqual(data.sort(), ['1.2.3.7','1.2.3.8'].sort());
        done();
    });
});

test('retrieves empty if there are no async slaves', function(done){
    MetaData.getDBAsyncSlaves('2.3.4.5', function(err, data){
        assert.deepEqual(data, []);
        done();
    });
});

    test('infowindow', function(done) {
        var req = {
            headers: {
                host: 'vizzuality.cartodb.com'
            },
            params: {
                table: 'public'
            }
        };
        MetaData.getInfowindow(req, function(err, info) {
            assert.equal(info, 'wadus');
            done();
        });
    });

    test('infowindow', function(done) {
        var req = {
            headers: {
                host: 'vizzuality.cartodb.com'
            },
            params: {
                table: 'public'
            }
        };
        MetaData.getMapMetadata(req, function(err, meta) {
            assert.equal(meta, 'foobar');
            done();
        });
    });

    test('can retrieve database connection params for username', function(done){
        MetaData.getUserDBConnectionParams('vizzuality', function(err, dbparams) {
            assert.equal(err, null, "Did not expect an err");
            assert.deepEqual(dbparams, {
                "dbhost": "localhost",
                "dbname": "cartodb_test_user_1_db",
                "dbuser": "publicuser"
            });
            done();
        });
    });

    test('fails to retrieve database connection params for invalid user', function(done){
        MetaData.getUserDBConnectionParams('fakeuser', function(err, dbparams) {
            assert.notEqual(err, null);
            assert.equal(dbparams, null);
            done();
        });
    });

    test('can retrieve all db params for username', function(done){
        MetaData.getAllUserDBParams('vizzuality', function(err, dbparams) {
            assert.equal(err, null, "Did not expect an err");
            assert.deepEqual(dbparams, {
                "dbhost": "localhost",
                "dbname": "cartodb_test_user_1_db",
                "dbpublicuser": "publicuser",
                "dbuser": "1",
                "dbpass": "secret",
                "apikey": "1234"
            });
            done();
        });
    });

    test('can retrieve all db params for username', function(done){
        MetaData.getUserDBPublicConnectionParams('vizzuality', function(err, dbparams) {
            assert.equal(err, null, "Did not expect an err");
            assert.deepEqual(dbparams, {
                "dbhost": "localhost",
                "dbname": "cartodb_test_user_1_db",
                "dbpublicuser": "publicuser"
            });
            done();
        });
    });

    test('can retrieve oauth values for given oauth key', function(done) {
        MetaData.getOAuthHash('l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR', function(err, oAuthValues) {
            assert.equal(err, null);
            assert.notEqual(oAuthValues, null);
            assert.equal(oAuthValues.consumer_key, 'fZeNGv5iYayvItgDYHUbot1Ukb5rVyX6QAg8GaY2');
            assert.equal(oAuthValues.consumer_secret, 'IBLCvPEefxbIiGZhGlakYV4eM8AbVSwsHxwEYpzx');
            assert.equal(oAuthValues.access_token_token, 'l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR');
            assert.equal(oAuthValues.access_token_secret, '22zBIek567fMDEebzfnSdGe8peMFVFqAreOENaDK');
            assert.equal(oAuthValues.time, 'sometime');
            assert.equal(oAuthValues.user_id, 1);
            done();
        });
    });

    test('invalid oauth keys returns empty object instead of null value', function(done) {
        MetaData.getOAuthHash('foo', function(err, oAuthValues) {
            assert.equal(err, null);
            assert.notEqual(oAuthValues, null);
            assert.deepEqual(oAuthValues, {});
            done();
        });
    });

    test('incMapviewCount increments both counters', function(done) {
        var getMapViewKeyFunc = MetaData.getMapViewKey;
        MetaData.getMapViewKey = function() {
            return '20140101';
        };
        MetaData.incMapviewCount('vizzuality', 'foo', function(err, values) {
            MetaData.getMapViewKey = getMapViewKeyFunc;

            assert.equal(values.length, 2, 'expected two values, got ' + values.length);
            var global = values[0];
            var tag = values[1];
            assert.equal(global, 2);
            assert.equal(tag, 1);

            done();
        })
    });

    test('log is called if elapsed time is above configured one', function(done) {
        var logWasCalled = false,
            elapsedThreshold = 25,
            enabledSlowQueriesConfig = {
                slowQueries: {
                    log: true,
                    elapsedThreshold: elapsedThreshold
                }
            };

        var times = 0;
        var dateNowFunc = Date.now;
        Date.now = function () {
            return times++ * elapsedThreshold * 2;
        };
        var consoleLogFunc = console.log;
        console.log = function(what) {
            var whatObj;
            try {
                whatObj = JSON.parse(what);
            } catch (e) {
                // pass
            }
            logWasCalled = whatObj && whatObj.action && whatObj.action === 'query';
            consoleLogFunc.apply(console, arguments);
        };

        var cartoMetadata = require('../lib/carto_metadata')(_.extend(redis_config, enabledSlowQueriesConfig));

        cartoMetadata.getAllUserDBParams('vizzuality', function(/*err, dbParams*/) {
            console.log = consoleLogFunc;
            Date.now = dateNowFunc;

            assert.ok(logWasCalled);

            done();
        })
    });

    test('can retrieve render limit and it is a number', function(done){
        MetaData.getTilerRenderLimit('vizzuality', function(err, renderLimit) {
            assert.ok(!err);
            assert.ok(_.isNumber(renderLimit));
            assert.equal(renderLimit, 2000);
            done();
        });
    });

    test('if render limit does not exist it returns as the stored value and not as number', function(done){
        MetaData.getTilerRenderLimit('nonexistent', function(err, renderLimit) {
            assert.ok(!err);
            assert.ok(!_.isNumber(renderLimit));
            assert.equal(renderLimit, null);
            done();
        });
    });

});
