#!/bin/sh

cd $(dirname $0)
BASEDIR=$(pwd)
cd -

REDIS_PORT=`node -e "console.log(require('${BASEDIR}/test/support/config').redis_pool.port)"`
export REDIS_PORT

cleanup_and_exit() {
	cleanup
	exit
}

die() {
	msg=$1
	echo "${msg}" >&2
	cleanup
	exit 1
}

trap 'cleanup_and_exit' 1 2 3 5 9 13

echo "Starting redis on port ${REDIS_PORT}"
REDIS_CELL_PATH="${BASEDIR}/test/support/libredis_cell.so"
if [ "$OSTYPE" == "darwin"* ]; then
  REDIS_CELL_PATH="${BASEDIR}/test/support/libredis_cell.dylib"
fi
echo "port ${REDIS_PORT}" | redis-server - --loadmodule ${REDIS_CELL_PATH} > ${BASEDIR}/test.log &
PID_REDIS=$!
echo ${PID_REDIS}
echo ${PID_REDIS} > ${BASEDIR}/redis.pid
sleep 1 # wait a bit for it to start (there must be a better way!)

echo "Preparing the environment"
cd ${BASEDIR}/test/support; sh prepare_db.sh || die "database preparation failure"; cd -
