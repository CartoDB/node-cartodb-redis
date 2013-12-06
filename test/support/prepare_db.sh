#!/bin/sh

# this script prepares and redis instance to run acceptance test
#
# NOTE: assumes existance of a "template_postgis" loaded with
#       compatible version of postgis (legacy.sql included)

# This is where postgresql connection parameters are read from
TESTENV=./config.js


TEST_DB="cartodb_test_user_1_db"
REDIS_PORT=`node -e "console.log(require('${TESTENV}').redis_pool.port || '6336')"`

export PGHOST PGPORT

die() {
        msg=$1
        echo "${msg}" >&2
        exit 1
}

echo "preparing redis..."
#TODO: pipeline
cat <<EOF | redis-cli -p ${REDIS_PORT} -n 5
HSET rails:users:vizzuality id 1
HSET rails:users:vizzuality database_name ${TEST_DB}
HSET rails:users:vizzuality database_host localhost
HSET rails:users:vizzuality database_password secret
HSET rails:users:vizzuality map_key 1234
SADD rails:users:vizzuality:map_key 1235
EOF

cat <<EOF | redis-cli -p ${REDIS_PORT} -n 0
HSET rails:cartodb_test_user_1_db:private privacy 0
HSET rails:cartodb_test_user_1_db:private the_geom_type point
HSET rails:cartodb_test_user_1_db:public privacy 1
HSET rails:cartodb_test_user_1_db:public the_geom_type geometry
EOF

cat <<EOF | redis-cli -p ${REDIS_PORT} -n 3
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR consumer_key fZeNGv5iYayvItgDYHUbot1Ukb5rVyX6QAg8GaY2
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR consumer_secret IBLCvPEefxbIiGZhGlakYV4eM8AbVSwsHxwEYpzx
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR access_token_token l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR access_token_secret 22zBIek567fMDEebzfnSdGe8peMFVFqAreOENaDK
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR user_id 1
HSET rails:oauth_access_tokens:l0lPbtP68ao8NfStCiA3V3neqfM03JKhToxhUQTR time sometime
EOF

echo "ok, you can run test now"


