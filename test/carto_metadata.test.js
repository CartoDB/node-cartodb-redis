var redis_config = require('./support/config').redis_pool;

var _ = require('underscore');
var MetaData = require('../lib/carto_metadata')(redis_config);
var assert = require('assert');
var strftime = require('strftime');

suite('metadata', function() {


test('can retrieve database name for username', function(done){
    MetaData.getUserDBName('vizzuality', function(err, data){
        assert.equal(data, 'cartodb_test_user_1_db');
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
        var table = 'public';

        MetaData.getUserDBName('vizzuality', function(err, dbname) {
            MetaData.getTableInfowindow(dbname, table, function(err, info) {
                assert.equal(info, 'wadus');
                done();
            });
        });
    });

    test('infowindow', function(done) {
        var table = 'public';

        MetaData.getUserDBName('vizzuality', function(err, dbname) {
            MetaData.getTableMapMetadata(dbname, table, function(err, meta) {
                assert.equal(meta, 'foobar');
                done();
            });
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

    test('incMapviewCount increments tree counters', function(done) {
        var getMapViewKeyFunc = MetaData.getMapViewKey;
        MetaData.getMapViewKey = function() {
            return '20140101';
        };
        MetaData.incMapviewCount('vizzuality', 'foo', function(err, values) {
            MetaData.getMapViewKey = getMapViewKeyFunc;

            assert.equal(values.length, 3, 'expected three values, got ' + values.length);
            var global = values[0];
            var tag = values[1];
            var tagTotal = values[2];
            assert.equal(global, 2);
            assert.equal(tag, 1);
            assert.equal(tagTotal, 1);

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

    test('can retrieve user timeout limit for public role and it is a number', function(done){
        MetaData.getUserTimeoutRenderLimits('vizzuality', function(err, timeoutLimit) {
            assert.ifError(err);
            assert.ok(_.isNumber(timeoutLimit.render));
            assert.equal(timeoutLimit.render, 5000);
            assert.ok(_.isNumber(timeoutLimit.renderPublic));
            assert.equal(timeoutLimit.renderPublic, 4000);
            done();
        });
    });

    var getMapViewKeyDays = [1,2,3,4,5,6,7,8,9,10,11,12,26,30,31];
    var getMapViewKeyMonths = [0, 11];

    getMapViewKeyMonths.forEach(function(month) {
        getMapViewKeyDays.forEach(function(day) {
            test('getMapViewKey format, month=' + month + '; day=' + day, function() {
                var d = new Date(2016, month, day);
                var mapViewKey = MetaData.getMapViewKey(d);
                assert.equal(mapViewKey, strftime("%Y%m%d", d));
            });
        });
    });

    test('should get an API key', function (done) {
        MetaData.getApikey('vizzuality', '1234', function (err, apikey) {
            assert.equal(err, null);
            assert.deepEqual(apikey, {
                user: 'vizzuality',
                type: "master",
                grantsSql: true,
                grantsMaps: true,
                databaseRole: "vizzuality_role",
                databasePassword: "vizzuality_password"
            });
            done();
        });
    });

    test('should get an user master API key', function (done) {
        MetaData.getMasterApikey('vizzuality', function (err, apikey) {
            assert.equal(err, null);
            assert.deepEqual(apikey, {
                user: 'vizzuality',
                type: "master",
                grantsSql: true,
                grantsMaps: true,
                databaseRole: "vizzuality_role",
                databasePassword: "vizzuality_password"
            });
            done();
        });
    });

    test('should return empty API key if it does not exist', function (done) {
        MetaData.getApikey('vizzuality', 'THIS_API_KEY_DOES_NOT_EXIST', function (err, apikey) {
            assert.equal(err, null);
            assert.deepEqual(apikey, {
                user: null,
                type: null,
                grantsSql: false,
                grantsMaps: false,
                databaseRole: null,
                databasePassword: null
            });
            done();
        });
    });
});
