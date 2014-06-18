#!/bin/sh

# this script prepares and redis instance to run acceptance test
#
# NOTE: assumes existance of a "template_postgis" loaded with
#       compatible version of postgis (legacy.sql included)

# This is where postgresql connection parameters are read from
TESTENV=./config.js


TEST_DB="cartodb_test_user_1_db"
REDIS_PORT=`node -e "console.log(require('${TESTENV}').redis_pool.port || '6336')"`

die() {
        msg=$1
        echo "${msg}" >&2
        exit 1
}

echo "preparing redis..."
cat <<EOF | redis-cli -p ${REDIS_PORT} -n 5
HMSET rails:users:vizzuality id 1 \
                             database_name ${TEST_DB} \
                             database_host localhost \
                             database_password secret map_key 1234
SADD rails:users:vizzuality:map_key 1235
EOF

cat <<EOF | redis-cli -p ${REDIS_PORT} -n 0
HMSET rails:cartodb_test_user_1_db:private privacy 0 \
                                           the_geom_type point
HMSET rails:cartodb_test_user_1_db:public privacy 1 \
                                          the_geom_type geometry
EOF

cat <<EOF | redis-cli -p ${REDIS_PORT} -n 3
HMSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR \
  consumer_key fZeNGv5iYayvItgDYHUbot1Ukb5rVyX6QAg8GaY2 \
  consumer_secret IBLCvPEefxbIiGZhGlakYV4eM8AbVSwsHxwEYpzx \
  access_token_token l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR \
  access_token_secret 22zBIek567fMDEebzfnSdGe8peMFVFqAreOENaDK \
  user_id 1 \
  time sometime 
EOF

cat <<EOF | redis-cli -p ${REDIS_PORT} -n 5
SADD db:1.2.3.4:sync_slaves 1.2.3.5 1.2.3.6
SADD db:1.2.3.4:async_slaves 1.2.3.7 1.2.3.8
EOF

echo "ok, you can run test now"


