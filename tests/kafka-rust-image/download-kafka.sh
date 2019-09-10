#!/bin/sh

set -ex

url="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
curl -Sso "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" "${url}"
