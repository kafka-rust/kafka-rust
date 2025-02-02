#!/bin/bash

if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi


echo "Starting kafka server"

create-topics.sh &

if [[ ! -z "$KAFKA_CLIENT_SECURE" ]]; then
  config_fname="$KAFKA_HOME/config/secure.server.properties"
else
  config_fname="$KAFKA_HOME/config/server.properties"
fi

set -x
exec $KAFKA_HOME/bin/kafka-server-start.sh $config_fname
