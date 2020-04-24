#!/bin/sh

cd $(dirname $0)
BASEDIR=$(pwd)
cd -

PID_REDIS=$(cat ${BASEDIR}/redis.pid)
if test x"$PID_REDIS" = x; then
  echo "Could not find a test redis pid to kill it"
  return;
fi
echo "Cleaning up"
echo ${PID_REDIS}
kill -9 ${PID_REDIS}
