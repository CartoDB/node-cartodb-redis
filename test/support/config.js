module.exports = function(opts) {

    var config = {
        redis_pool: {
            max: 10,
            idleTimeoutMillis: 1,
            reapIntervalMillis: 1,
            port: 6336
        }
    }

    Object.assign(config,  opts || {});

    return config;
}();

