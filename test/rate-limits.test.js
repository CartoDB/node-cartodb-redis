var assert = require('assert');
var redis_config = require('./support/config').redis_pool;
var MetaData = require('../lib/carto_metadata')(redis_config);

var user = 'vizzuality';
var app = 'test';
var endpointGroup = 'test';

describe('rate limits: getLowerRateLimit', function() {
    it("1 limit: not limited", function () {
        var limits = [[0, 3, 1, -1, 1]];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limits[0], result);
    });

    it("1 limit: limited", function () {
        var limits = [[1, 3, 0, 0, 1]];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limits[0], result);
    });

    it("empty or invalid", function () {
        var limits = [];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = undefined;
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = null;
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = [[]];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = [[], []];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = {};
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = [{}];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);

        limits = [[1, 2]];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(undefined, result);
    });

    it("multiple limits: valid and invalid", function () {
        var limit1 = [0, 3, 0];
        var limit2 = [0, 3, 1, 0, 1];

        var limits = [limit1, limit2];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit2, result);

        limits = [limit2, limit1];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit2, result);
    });

    it("multiple limits: not limited", function () {
        var limit1 = [0, 3, 2, 0, 1];
        var limit2 = [0, 3, 3, 0, 1];
        var limit3 = [0, 3, 1, 0, 1];
        var limit4 = [0, 3, 4, 0, 1];
        var limit5 = [0, 3, 5, 0, 1];

        var limits = [limit1, limit2, limit3, limit4, limit5];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit3, result);

        limits = [limit1, limit2];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit1, result);
    });

    it("multiple limits: limited", function () {
        var limit1 = [0, 3, 2, 0, 1];
        var limit2 = [0, 3, 3, 0, 1];
        var limit3 = [0, 3, 1, 0, 1];
        var limit4 = [0, 3, 4, 0, 1];
        var limit5 = [1, 3, 5, 0, 1];

        var limits = [limit1, limit2, limit3, limit4, limit5];
        var result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit5, result);

        limits = [limit1, limit2, limit5, limit3, limit4];
        result = MetaData.getLowerRateLimit(limits);
        assert.deepEqual(limit5, result);
    });
});

function assertGetRateLimitRequest(isBlocked, limit, remaining, retry, reset, done) {
    MetaData.getRateLimit(user, app, endpointGroup, function (err, rateLimit) {
        assert.ifError(err);
        assert.deepEqual(rateLimit, [isBlocked, limit, remaining, retry, reset]);

        if (done) {
            setTimeout(done, reset * 1000 + 1000)
        }
    });
}

describe('rate limits: getRateLimit', function() {
    before(function(done) {
        var storeKey = MetaData.rate_limits_store_key({
            username: user,
            app: app,
            endpointGroup: endpointGroup
        });

        MetaData.redisCmd(
            MetaData.rate_limits_db,
            'RPUSH',
            [storeKey, 1, 1, 1],
            done
        )
    });

    after(function(done) {
        var storeKey = MetaData.rate_limits_store_key({
            username: user,
            app: app,
            endpointGroup: endpointGroup
        });

        MetaData.redisCmd(
            MetaData.rate_limits_db,
            'DEL',
            [storeKey],
            done
        )
    });

    it("1 req/sec: 2 req/seg should be limited", function (done) {
        this.timeout(5000);

        assertGetRateLimitRequest(0, 2, 1, -1, 1);
        setTimeout( function() { assertGetRateLimitRequest(0, 2, 0, -1, 1)}, 250);
        setTimeout( function() { assertGetRateLimitRequest(1, 2, 0, 0, 1) }, 500);
        setTimeout( function() { assertGetRateLimitRequest(1, 2, 0, 0, 1) }, 750);
        setTimeout( function() { assertGetRateLimitRequest(1, 2, 0, 0, 1) }, 950);
        setTimeout( function() { assertGetRateLimitRequest(0, 2, 0, -1, 1, done) }, 1050);
    });

    it('should work with loadRateLimitsScript', function (done) {
        this.timeout(5000);

        MetaData.loadRateLimitsScript(function(err) {
            assert.ifError(err);
            assertGetRateLimitRequest(0, 2, 1, -1, 1, done);
        });
    });

    it('should work after lose loadRateLimitsScript', function (done) {
        this.timeout(5000);

        MetaData.loadRateLimitsScript(function(err) {
            assert.ifError(err);

            MetaData.redisCmd(MetaData.rate_limits_db, 'SCRIPT', ['FLUSH'], function () {
                assertGetRateLimitRequest(0, 2, 1, -1, 1, done);
            });
        });
    });
});

describe('rate limits: getRateLimit with several limits', function() {
    before(function(done) {
        var storeKey = MetaData.rate_limits_store_key({
            username: user,
            app: app,
            endpointGroup: endpointGroup
        });

        MetaData.redisCmd(
            MetaData.rate_limits_db,
            'RPUSH',
            [storeKey, 10, 10, 1, 60, 120, 60],
            done
        )
    });

    after(function(done) {
        var storeKey = MetaData.rate_limits_store_key({
            username: user,
            app: app,
            endpointGroup: endpointGroup
        });

        MetaData.redisCmd(
            MetaData.rate_limits_db,
            'DEL',
            [storeKey],
            done
        )
    });

    it("should removing one every request", function (done) {
        this.timeout(5000);

        assertGetRateLimitRequest(0, 11, 10, -1, 0);
        assertGetRateLimitRequest(0, 11, 9, -1, 0);
        assertGetRateLimitRequest(0, 11, 8, -1, 0);
        assertGetRateLimitRequest(0, 11, 7, -1, 0);
        assertGetRateLimitRequest(0, 11, 6, -1, 0);
        assertGetRateLimitRequest(0, 11, 5, -1, 0);
        assertGetRateLimitRequest(0, 11, 4, -1, 0);
        assertGetRateLimitRequest(0, 11, 3, -1, 0);
        assertGetRateLimitRequest(0, 11, 2, -1, 0);
        assertGetRateLimitRequest(0, 11, 1, -1, 0);
        assertGetRateLimitRequest(0, 11, 0, -1, 1);
        assertGetRateLimitRequest(1, 11, 0, 0, 1, done);
    });
});

describe('rate limits: invalid limits', function() {
    it("should no return a rate limit if limit values are empty", function (done) {
        var storeKey = MetaData.rate_limits_store_key({
            username: user,
            app: app,
            endpointGroup: endpointGroup
        });

        MetaData.redisCmd(
            MetaData.rate_limits_db,
            'DEL',
            [storeKey],
            function() {
                MetaData.getRateLimit(user, app, endpointGroup, function (err, rateLimit) {
                    assert.ifError(err);
                    assert.ifError(rateLimit);
                    done();
                });
            }
        )

    });

});
