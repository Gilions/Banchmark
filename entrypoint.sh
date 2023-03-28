#!/bin/sh
cmd="$@"
ACCESS=false

while ! $ACCESS;
do
  if ! nc -z -v $CH_HOST $CH_PORT
    then
      >&2 echo "ClickHouse is not available, waiting 3 sec!"
      sleep 3;
  elif ! nc -z -v $MONGO_HOST $MONGO_PORT
    then
      >&2 echo "MongoDB is not available, waiting 3 sec!"
      sleep 3;
  else
    ACCESS=true
  fi
done
exec
$cmd