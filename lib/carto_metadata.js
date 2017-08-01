/**
 * User: simon
 * Date: 30/08/2011
 * Time: 21:10
 * Desc: CartoDB helper.
 *       Retrieves dbname (based on subdomain/username)
 *       and geometry type from the redis stores of cartodb
 */

var RedisPool = require('redis-mpool');
var _ = require('underscore');
var dot = require('dot');

//
// @param redis_opts configuration option for redis.
//   recognized members for redis_opts are:
//     host -- hostname or IP address defaults to 127.0.0.1
//     port -- TCP port number, defaults to 6379
//     max  -- max elements in pool, defaults to 50
//     idleTimeoutMillis -- timeout for idle connections, defaults to 10000
//     reapIntervalMillis -- timeout check interval, defaults to 1000
//     log -- should activities be logged ? defaults to false
//
module.exports = function(redis_opts) {
    var defaults = {
        slowQueries: {
            log: false,
            elapsedThreshold: 25
        }
    };

    var options = _.defaults(redis_opts, defaults);

    var redis_pool = (options.pool)
        ? options.pool
        : new RedisPool(_.extend(options, {name: 'metadata'}));

    var logSlowQueries = options.slowQueries.log,
        slowQueriesElapsedThreshold = options.slowQueries.elapsedThreshold;


    var me = {
        user_metadata_db: 5,
        db_metadata_db: 5,
        table_metadata_db: 0,
        oauth_metadata_db: 3,
        user_key: dot.template("rails:users:{{=it.username}}"),
        oauth_user_key: dot.template("rails:oauth_access_tokens:{{=it.oauth_access_key}}"),
        table_key: dot.template("rails:{{=it.database_name}}:{{=it.table_name}}"),
        sync_slaves_key: dot.template("db:{{=it.dbhost}}:sync_slaves"),
        async_slaves_key: dot.template("db:{{=it.dbhost}}:async_slaves"),
        global_mapview_key: dot.template("user:{{=it.username}}:mapviews:global"),
        tagged_mapview_key: dot.template("user:{{=it.username}}:mapviews:stat_tag:{{=it.stat_tag}}"),
        // limits
        user_tiler_limits: dot.template("limits:tiler:{{=it.username}}"),
        user_timeout_limits: dot.template("limits:timeout:{{=it.username}}") // db & render timeouts
    };

    /// @deprecated in 0.3.0
    me.userFromHostname = function(hostname) {
        console.error("WARNING: cartodb-redis: userFromHostname is deprecated");
        return hostname.split('.')[0];
    };

    /**
     * Get the privacy setting of a table
     *
     * @param dbname - database name, see getUserDBName
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy)
     */
    me.getTablePrivacy = function(dbname, tablename, callback) {
        var redisKey = this.table_key({database_name:dbname, table_name:tablename});
        this.retrieve(this.table_metadata_db, redisKey, 'privacy', callback);
    };

    /**
     * Get the geometry type setting of a table
     *
     * @param dbname - database name, see getUserDBName
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy)
     */
    me.getTableGeometryType = function(dbname, tablename, callback){
         var redisKey = this.table_key({database_name:dbname, table_name:tablename});
         this.retrieve(this.table_metadata_db, redisKey, 'the_geom_type', callback);
    };

    /**
     * Get the database name for this particular username
     *
     * @param username - cartodb username
     * @param callback - gets called with args(err, dbname)
     */
    me.getUserDBName = function(username, callback) {
        var redisKey = this.user_key({username: username});

        this.retrieve(this.user_metadata_db, redisKey, 'database_name', function(err, dbname) {
          if ( err ) callback(err, null);
          else if ( dbname === null ) {
            callback(new Error("missing " + username + "'s database_name in redis (try CARTODB/script/restore_redis)"), null);
          }
          else callback(err, dbname);
        });
    };

    /**
     * Get the database name for this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback - gets called with args(err, dbname)
     *
     * @deprecated in 0.2.0 use getUserDBName
     */
    me.getDatabase = function(req, callback) {

        console.error("WARNING: cartodb-redis: getDatabase is deprecated, use getUserDBName");

        // strip subdomain from header host
        var username = this.userFromHostname(req.headers.host);
        this.getUserDBName(username, callback);
    };

    /**
     * Increment mapview count for a user
     *
     * @param username
     * @param stat_tag
     * @param callback will be called with the new value
     */
    me.incMapviewCount = function(username, stat_tag, callback) {
        var that = this,
            mapViewKey = this.getMapViewKey(),
            globalKey = this.global_mapview_key({username: username});

        if (!stat_tag) {
            var redisKey = that.global_mapview_key({username: username});
            that.redisCmd(me.user_metadata_db, 'ZINCRBY', [redisKey, 1, mapViewKey], callback);
        } else {
            var tagKey = that.tagged_mapview_key({username: username, stat_tag: stat_tag});

            that.redisMultiCmd(me.user_metadata_db, [
                ['ZINCRBY', globalKey, 1, mapViewKey],
                ['ZINCRBY', tagKey, 1, mapViewKey],
                ['ZINCRBY', tagKey, 1, 'total']
            ], callback);
        }
    };

    var mapViewKeyTpl = dot.template([
        '{{=it.year}}',
        '{{?it.month < 10}}0{{?}}{{=it.month}}',
        '{{?it.day < 10}}0{{?}}{{=it.day}}'
    ].join(''));
    me.getMapViewKey = function(d) {
        d = d || new Date();
        return mapViewKeyTpl({
            year: d.getFullYear(),
            month: d.getMonth() + 1,
            day: d.getDate()
        });
    };

    /**
     * Get the user id for this particular username
     *
     * @param username - cartodb username
     * @param callback
     */
    me.getUserId = function(username, callback) {
        var redisKey = this.user_key({username: username});

        this.retrieve(this.user_metadata_db, redisKey, 'id', function(err, dbname) {
          if ( err ) callback(err, null);
          else if ( dbname === null ) {
            callback(new Error("missing " + username + "'s id in redis (try CARTODB/script/restore_redis)"), null);
          }
          else callback(err, dbname);
        });
    };

    /**
     * Get the user id for this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getUserId
     */
    me.getId= function(req, callback) {

        console.error("WARNING: cartodb-redis: getId is deprecated, use getUserId");

        // strip subdomain from header host
        var username = this.userFromHostname(req.headers.host);
        this.getUserId(username, callback);
    };

    /**
     * Get the database host this particular /username
     *
     * @param username - cartodb username
     * @param callback
     */
    me.getUserDBHost = function(username, callback) {
        var redisKey = this.user_key({username: username});

        this.retrieve(this.user_metadata_db, redisKey, 'database_host', function(err, dbname) {
          if ( err ) callback(err, null);
          else {
            if ( dbname === null ) {
              /* database_host was introduced in cartodb-2.5.0,
               * for older versions we'll just use configured host */
              //console.log("WARNING: missing " + username + "'s database_host in redis (try CARTODB/script/restore_redis)");
            }
            callback(err, dbname);
          }
        });
    };

    me.getUserDBConnectionParams = function(username, callback) {
        var dbParams = {
            database_host: 'dbhost',
            database_name: 'dbname',
            database_publicuser: 'dbuser'
        };
        this.getMultipleUserDBParams(username, dbParams, callback);
    };

    me.getAllUserDBParams = function (username, callback) {
        var dbParams = {
            database_host: 'dbhost',
            database_name: 'dbname',
            database_publicuser: 'dbpublicuser',
            id: 'dbuser',
            database_password: 'dbpass',
            map_key: 'apikey'
        };
        this.getMultipleUserDBParams(username, dbParams, callback);
    };

    me.getUserDBPublicConnectionParams = function(username, callback) {
        var dbParams = {
            database_host: 'dbhost',
            database_name: 'dbname',
            database_publicuser: 'dbpublicuser'
        };
        this.getMultipleUserDBParams(username, dbParams, callback);
    };

    me.getMultipleUserDBParams = function(username, dbParams, callback) {
        var redisKey = this.user_key({username: username}),
            dbParamsKeys = Object.keys(dbParams);

        this.mRetrieve(this.user_metadata_db, redisKey, dbParamsKeys, function(err, dbValues) {
            if ( err ) {
                callback(err, null);
            } else {
                var paramsMap = {};
                for (var i = 0, len = dbParamsKeys.length; i < len; i++) {
                    paramsMap[dbParams[dbParamsKeys[i]]] = dbValues[i];
                }
                if (paramsMap.dbhost == null || paramsMap.dbname == null) {
                    callback(new Error("DB host or name not found in redis (try CARTODB/script/restore_redis)"), null);
                } else {
                    callback(err, paramsMap);
                }
            }
        });
    };

    me.getOAuthHash = function(oAuthAccessKey, callback) {
        var redisKey = this.oauth_user_key({oauth_access_key: oAuthAccessKey});
        this.redisCmd(this.oauth_metadata_db, 'HGETALL', [redisKey], function(err, oAuthValues) {
            if ( ! err && oAuthValues === null ) {
                oAuthValues = {};
            }
            callback(err, oAuthValues);
        });
    };

    /**
     * Get the database host this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getUserDBHost
     */
    me.getDatabaseHost= function(req, callback) {

        console.error("WARNING: cartodb-redis: getDatabaseHost is deprecated, use getUserDBHost");

        // strip subdomain from header host
        var username = this.userFromHostname(req.headers.host);
        this.getUserDBHost(username, callback);
    };

    /**
     * Get the database password for this particular username
     *
     * @param username - cartodb username
     * @param callback
     */
    me.getUserDBPass = function(username, callback) {
        var redisKey = this.user_key({username: username});

        this.retrieve(this.user_metadata_db, redisKey, 'database_password', function(err, data) {
          if ( err ) callback(err, null);
          else {
            if ( data === null ) {
              /* database_password was introduced in cartodb-2.5.0,
               * for older versions we'll just use configured password */
              //console.log("WARNING: missing " + username + "'s database_password in redis (try CARTODB/script/restore_redis)");
            }
            callback(err, data);
          }
        });
    };

    /**
     * Get the database password for this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getUserDBPass
     */
    me.getDatabasePassword= function(req, callback) {

        console.error("WARNING: cartodb-redis: getDatabasePassword is deprecated, use getUserDBPass");

        // strip subdomain from header host
        var username = this.userFromHostname(req.headers.host);
        this.getUserDBPass(username, callback);
    };

    /**
     * Get the api key for this particular username
     *
     * @param username - cartodb username
     * @param callback function(err,val)
     *
     */
    me.getUserMapKey = function(username, callback) {
        var redisKey = this.user_key({username: username});
        this.retrieve(this.user_metadata_db, redisKey, "map_key", callback);
    };

    /**
     * Check the user map key for this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @deprecated in 0.3.0 use getUserMapKey
     */
    me.checkMapKey = function(req, callback) {

        console.error("WARNING: cartodb-redis: checkMapKey is deprecated, use getUserMapKey");

        // strip subdomain from header host
        var username = this.userFromHostname(req.headers.host);
        this.getUserMapKey(username, function(err, val) {
            var valid = 0;
            if ( val ) {
              if ( val == req.query.map_key ) valid = 1;
              else if ( val == req.query.api_key ) valid = 1;
              // check also in request body
              else if ( req.body && req.body.map_key && val == req.body.map_key ) valid = 1;
              else if ( req.body && req.body.api_key && val == req.body.api_key ) valid = 1;
            }
            callback(err, valid);
        });
    };
    // @deprecated in 0.3.0 use getUserMapKey
    me.checkAPIKey = me.checkMapKey;

    /**
     * Get the geometry type for this particular table;
     * @param req - standard req object. Importantly contains table and host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getTableGeometryType
     */
    me.getGeometryType = function(req, callback){

        console.error("WARNING: cartodb-redis: getGeometryType is deprecated, use getTableGeometryType");

        var that = this;
        this.getDatabase(req, function(err, dbname) {
            if (err) {
                callback(err);
            } else {
                that.getTableGeometryType(dbname, req.params.table, callback);
            }
        });

    };

    /**
     * Get the infowindow configuration for a table;
     *
     * @param dbname - database name, see getUserDBName
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy)
     */
    me.getTableInfowindow = function(dbname, tablename, callback){
         var redisKey = this.table_key({database_name:dbname, table_name:tablename});
         this.retrieve(this.table_metadata_db, redisKey, 'infowindow', callback);
    };


    /**
     * Get the infowindow configuration for this particular table;
     *
     * @param req - standard req object. Importantly contains table and host information
     * @param callback
     *
     * @deprecated in 0.3.0 use getTableInfowindow
     */
    me.getInfowindow = function(req, callback){
        var that = this;

        console.error("WARNING: cartodb-redis: getInfowindow is deprecated, use getTableInfowindow");

        this.getDatabase(req, function(err, dbname) {
            if (err) {
                callback(err);
            } else {
                that.getTableInfowindow(dbname, req.params.table, callback);
            }
        });
    };

    /**
     * Get the synchronous slaves for a database host.
     * Those slaves are assumed to be synchronized with the master.
     * If they exist, they are only to be used for read queries.
     *
     * @param dbhost - database host of the master database to lookup
     * @param callback - gets called with args(err, slaves)
     */
    me.getDBSyncSlaves = function(dbhost, callback){
        var redisKey = this.sync_slaves_key({dbhost:dbhost});
        this.redisCmd(this.db_metadata_db,'SMEMBERS',[redisKey],callback);
    };

    /**
     * Get the asynchronous slaves for a database host.
     * Those slaves aren't guaranteed to be synchronized with the master
     * due to replication lag, which should be taken into account.
     * If they exists, they are only be used for read queries.
     *
     * @param dbhost - database host of the master database to lookup
     * @param callback - gets called with args(err, slaves)
     */
    me.getDBAsyncSlaves = function(dbhost, callback){
        var redisKey = this.async_slaves_key({dbhost:dbhost});
        this.redisCmd(this.db_metadata_db,'SMEMBERS',[redisKey],callback);
    };

    /**
     * Get the map_metadata for a table;
     *
     * @param dbname - database name, see getUserDBName
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy)
     */
    me.getTableMapMetadata = function(dbname, tablename, callback){
         var redisKey = this.table_key({database_name:dbname, table_name:tablename});
         this.retrieve(this.table_metadata_db, redisKey, 'map_metadata', callback);
    };


    // @deprecated in 0.2.0, can use getTableMapMetadata as of 0.3.0
    me.getMapMetadata = function(req, callback){
        var that = this;

        console.error("WARNING: cartodb-redis: getMapMetadata is deprecated, use getTableMapMetadata");

        this.getDatabase(req, function(err, dbname) {
            if (err) {
                callback(err);
            } else {
                that.getTableMapMetadata(dbname, req.params.table, callback);
            }
        });
    };

    /*******************************************************************************************************************
     * LIMITS
     ******************************************************************************************************************/

    me.getTilerRenderLimit = function(username, callback) {
        var userLimitsKey = this.user_tiler_limits({username: username});
        this.retrieve(this.user_metadata_db, userLimitsKey, 'render', function(err, renderLimit) {
            renderLimit = (!!renderLimit) ? +renderLimit : renderLimit;
            callback(err, renderLimit);
        });
    };

    me.getMultipleUserLimitsParams = function(username, limitsParams, callback) {
        var redisKey = this.user_timeout_limits({ username: username });
        var limitsParamsKeys = Object.keys(limitsParams);

        this.mRetrieve(this.user_metadata_db, redisKey, limitsParamsKeys, function(err, limitsValues) {
            if ( err ) {
                return callback(err);
            }

            var paramsMap = {};

            for (var i = 0, len = limitsParamsKeys.length; i < len; i++) {
                paramsMap[limitsParams[limitsParamsKeys[i]]] = (!!limitsValues[i]) ? +limitsValues[i] : limitsValues[i];
            }

            callback(null, paramsMap);
        });
    };

    me.getUserTimeoutRenderLimits = function(username, callback) {
        var limitsParams = {
            render: 'render',
            render_public: 'renderPublic'
        };

        this.getMultipleUserLimitsParams(username, limitsParams, callback);
    };

    /*******************************************************************************************************************
     * END LIMITS
     ******************************************************************************************************************/

    // Redis Hash lookup
    // @param callback will be invoked with args (err, reply)
    //                 note that reply is null when the key is missing
    me.retrieve = function(db, redisKey, hashKey, callback) {
        this.redisCmd(db,'HGET',[redisKey, hashKey], callback);
    };

    me.mRetrieve = function (db, redisKey, hashKeys, callback) {
        this.redisCmd(db, 'HMGET', [redisKey, hashKeys], callback);
    };

    // Redis Set member check
    me.inSet = function(db, setKey, member, callback) {
        this.redisCmd(db,'SISMEMBER',[setKey, member], callback);
    };

    // Redis INCREMENT
    me.increment = function(db, key, callback) {
        this.redisCmd(db,'INCR', key, callback);
    };

    /**
     * Use Redis
     *
     * @param db - redis database number
     * @param redisFunc - the redis function to execute
     * @param redisArgs - the arguments for the redis function in an array
     * @param callback - function to pass results too.
     */
    me.redisCmd = function(db, redisFunc, redisArgs, callback) {
        genericCommand(db, function(client, commands, done) {
            commands.push(redisFunc);
            redisArgs.push(done);
            client[redisFunc.toUpperCase()].apply(client, redisArgs);
        }, callback);
    };

    me.redisMultiCmd = function (db, redisCmdsAndArgs, callback) {
        genericCommand(db, function(client, commands, done) {
            redisCmdsAndArgs.forEach(function(cmd) {
                commands.push(cmd[0]);
            });
            client.multi(redisCmdsAndArgs).exec(done);
        }, callback)
    };

    function genericCommand(db, execQueryFunc, callback) {
        var redisClient,
            commands = [],
            executeStartTime;

        redis_pool.acquire(db, function(err, client) {
            if (err) {
                callback(err);
            } else {
                redisClient = client;
                executeStartTime = Date.now();
                execQueryFunc(client, commands, function(err, data) {
                    if (logSlowQueries) {
                        var elapsedTime = Date.now() - executeStartTime;
                        if (elapsedTime > slowQueriesElapsedThreshold) {
                            console.log(
                                JSON.stringify({db: db, action: 'query', commands: commands, elapsed: elapsedTime})
                            );
                        }
                    }
                    if (redisClient) {
                        redis_pool.release(db, redisClient);
                    }
                    callback(err, data);
                });
            }
        });
    }

    return me;
};
