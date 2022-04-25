#!/bin/bash

start_docker() {
  # pull zookeeper and build the kafka image
  docker-compose pull
  docker-compose build kafka
  docker-compose up -d zookeeper kafka
}

stop_docker() {
  docker-compose down
}

if [ $# -eq 0 ]
then
    echo "No arguments supplied"
fi

echo %1

if [ $1 = "start" ] 
then
    #start_docker()
    echo "START STart"
fi

if [ $1 = "stop" ] 
then
    stop_docker()
fi