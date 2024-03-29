3.0.1 -- 2021-11-24
--------------------
- Add export limit

3.0.0 -- 2020-05-13
--------------------
- Upgrade `redis-mpool` version to 0.8.0
- Support Node.js 12
- Drop support for Node.js < 12

2.1.0 -- 2018-11-21
--------------------
- Support Node.js 8 and 10
- Add package-lock.json

2.0.2 -- 2018-10-25
--------------------
- Make all modules to use strict mode semantics.

2.0.1 -- 2018-07-05
--------------------
- Fix rate limit of an endpoint with several limits (#27)

2.0.0 -- 2018-07-05
--------------------
- Breaking changes:
    - Removing deprecated methods: getId, getDatabase, getDatabaseHost, getDatabasePassword, getMapMetadata,
      getGeometryType, getInfowindow, checkMapKey, checkAPIKey and userFromHostname
- Upgrades mocha to 5.2.0

1.0.1 -- 2018-06-07
--------------------
- Remove Auth Fallback

1.0.0 -- 2018-03-19
--------------------
- Breaking change:
    - Needs Redis v4
- Rate limit functions
- Updating minimum node version to 6.9.0 (it is not a breaking change, for now)

0.16.0 -- 2018-02-28
--------------------
- Add functions to get API keys

0.15.0 -- 2018-02-05
--------------------
- Upgrades redis-mpool to 0.5.0

0.14.0 -- 2017-08-08
--------------------
 - Add functions to get user's timeout limits

0.13.2 -- 2016-12-16
--------------------
- Upgrades redis-mpool to 0.4.1.

0.13.1 -- 2016-07-13
--------------------
- Removes strftime dependency.
- Upgrades redis-mpool to 0.4.0.

0.13.0 -- 2015-05-12
--------------------
- Increment stat_tag total counter besides date ones

0.12.1 -- 2015-04-07
--------------------
- Render time limit only returns as number if there is a value for the user

0.12.0 -- 2015-04-01
--------------------
- Adds a method to retrieve the render time limit

0.11.0 -- 2014-09-16
--------------------
- Do not log slow queries by default
- Remove step dependency
- Replace underscore templates with a more performant module for templating [doT](https://github.com/olado/doT)

0.10.0 -- 2014-08-19
--------------------
- Add test for map views
- Add support to log slow queries

0.9.0 -- 2014-08-13
-------------------
- Upgrades dependencies:
    - redis-mpool
    - underscore
    - strftime
- Specifies name in the redis pool

0.8.0 -- 2014-08-06
-------------------
- New method to retrieve oauth hash values
- Upgrades redis-mpool dependency to 0.0.4

0.7.0 -- 2014-08-05
-------------------
- New method to retrieve public connection database params

0.6.0 -- 2014-07-29
-------------------
- Adds support to multiple commands
- Use multiple commands in incrementing mapviews method
- New method to retrieve all database params for a given user

0.5.0 -- 2014-07-03
-------------------
 - New method to retrieve database connection params

0.4.0 -- 2014-06-23
-------------------
 - New methods: getDBSyncSlaves, getDBAsyncSlaves
 - Switch to 3-clause BSD license (#8)

0.3.0 -- 2013-12-16
-------------------

 - Use cartodb/node-mredis for redis pooling (#4)
 - New methods: getTableInfowindow, getTableMapMetadata
 - Deprecate methods: getInfowindow, getMapMetadata, checkMapKey,
                      userFromHostname

0.2.0 -- 2013-12-06
-------------------

 - New methods: getUserId, getUserDBName, getUserDBHost,
                getUserDBPass, getUserMapKey, getTableGeometryType
 - Deprecate methods: getId, getDatabase, getDatabaseHost,
                      getDatabasePassword, getMapMetadata,
                      getGeometryType

0.1.0 -- 2013-11-15
-------------------

 - Add API test (#1)
 - Add getTablePrivacy, drop .authorize
 - Turn CartoDBMetadata to a real class (#3)
 - Add checkAPIKey alias for checkMapKey

0.0.1 -- 2013-11-15
-------------------

Initial release
