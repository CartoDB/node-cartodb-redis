/**
 * User: simon
 * Date: 30/08/2011
 * Time: 21:10
 * Desc: CartoDB helper.
 *       Retrieves dbname (based on subdomain/username)
 *       and geometry type from the redis stores of cartodb
 */

var strftime    = require('strftime');

var   RedisPool = require("redis-mpool")
    , _ = require('underscore')
    , Step = require('step');

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
    var redis_pool = new RedisPool(redis_opts);


    var me = {
        user_metadata_db: 5,
        table_metadata_db: 0,
        user_key:  "rails:users:<%= username %>",
        table_key: "rails:<%= database_name %>:<%= table_name %>",
        global_mapview_key: "user:<%= username %>:mapviews:global",
        tagged_mapview_key: "user:<%= username %>:mapviews:stat_tag:<%= stat_tag %>"
    };

    me.userFromHostname = function(hostname) {
        return hostname.split('.')[0];
    }

    /**
     * Get the privacy setting of a table
     *
     * @param dbname - database name, see getUserDBName 
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy) 
     */
    me.getTablePrivacy = function(dbname, tablename, callback) {
        var redisKey = _.template(this.table_key, {database_name:dbname, table_name:tablename});
        this.retrieve(this.table_metadata_db, redisKey, 'privacy', callback);
    },

    /**
     * Get the geometry type setting of a table
     *
     * @param dbname - database name, see getUserDBName
     * @param tablename - name of the table
     * @param callback - gets called with args(err, privacy) 
     */
    me.getTableGeometryType = function(dbname, tablename, callback){
         var redisKey = _.template(this.table_key, {database_name:dbname, table_name:tablename});
         this.retrieve(this.table_metadata_db, redisKey, 'the_geom_type', callback);
    };

    /**
     * Get the database name for this particular username
     *
     * @param username - cartodb username
     * @param callback - gets called with args(err, dbname) 
     */
    me.getUserDBName = function(username, callback) {
        var redisKey = _.template(this.user_key, {username: username});

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
        var that = this;
        var now = strftime("%Y%m%d", new Date());
        var redisKey;
        Step (
          function incrementGlobal() {
            redisKey = _.template(that.global_mapview_key, {username: username});
            that.redisCmd(me.user_metadata_db, 'ZINCRBY', [redisKey, 1, now], this);
          },
          function incrementTag(err, val) {
            if ( err ) throw err;
            if ( _.isUndefined(stat_tag) ) return 1;
            redisKey = _.template(that.tagged_mapview_key, {username: username, stat_tag: stat_tag});
            that.redisCmd(me.user_metadata_db, 'ZINCRBY', [redisKey, 1, now], this);
          },
          function finish(err, val) {
            if ( callback ) callback(err);
          }
        );
    };

    /**
     * Get the user id for this particular username
     *
     * @param username - cartodb username
     * @param callback
     */
    me.getUserId = function(username, callback) {
        var redisKey = _.template(this.user_key, {username: username});

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
        var redisKey = _.template(this.user_key, {username: username});

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

    /**
     * Get the database host this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getUserDBHost
     */
    me.getDatabaseHost= function(req, callback) {
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
        var redisKey = _.template(this.user_key, {username: username});

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
        var redisKey = _.template(this.user_key, {username: username});
        this.retrieve(this.user_metadata_db, redisKey, "map_key", callback);
    };

    /**
     * Check the user map key for this particular subdomain/username
     *
     * @param req - standard express req object. importantly contains host information
     * @param callback
     *
     * @todo  deprecate ?
     */
    me.checkMapKey = function(req, callback) {
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
    me.checkAPIKey = me.checkMapKey;

    /**
     * Get the geometry type for this particular table;
     * @param req - standard req object. Importantly contains table and host information
     * @param callback
     *
     * @deprecated in 0.2.0 use getTableGeometryType
     */
    me.getGeometryType = function(req, callback){
        var that = this;

        Step(
            function(){
                that.getDatabase(req, this)
            },
            function(err, data){
                if (err) throw err;
                that.getTableGeometryType(data, req.params.table, this);
            },
            function(err, data){
                callback(err, data);
            }
        );
    };


    me.getInfowindow = function(req, callback){
        var that = this;

        Step(
            function(){
                that.getDatabase(req, this);
            },
            function(err, data) {
                if (err) throw err;
                var redisKey = _.template(that.table_key, {database_name: data, table_name: req.params.table});
                that.retrieve(that.table_metadata_db, redisKey, 'infowindow', this);
            },
            function(err, data){
                callback(err, data);
            }
        );
    };


    // @deprecated in 0.2.0 (not needed)
    // @todo write differently when needed, taking dbname/tablename
    me.getMapMetadata = function(req, callback){
        var that = this;

        Step(
            function(){
                that.getDatabase(req, this);
            },
            function(err, data) {
                if (err) throw err;
                var redisKey = _.template(that.table_key, {database_name: data, table_name: req.params.table});

                that.retrieve(that.table_metadata_db, redisKey, 'map_metadata', this);
            },
            function(err, data){
                callback(err, data);
            }
        );
    };

    // Redis Hash lookup
    // @param callback will be invoked with args (err, reply)
    //                 note that reply is null when the key is missing
    me.retrieve = function(db, redisKey, hashKey, callback) {
        this.redisCmd(db,'HGET',[redisKey, hashKey], callback);
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
        var redisClient;

        Step(
            function getRedisClient() {
                redis_pool.acquire(db, this);
            },
            function executeQuery(err, data) {
                if ( err ) throw err;
                redisClient = data;
                redisArgs.push(this);
                redisClient[redisFunc.toUpperCase()].apply(redisClient, redisArgs);
            },
            function releaseRedisClient(err, data) {
                if ( ! _.isUndefined(redisClient) ) redis_pool.release(db, redisClient);
                callback(err, data);
            }
        );
    };

    return me;
}
