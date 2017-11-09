#!/bin/sh

set -ex

mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | jq -r '.preferred')
url="${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
curl -Sso "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" "${url}" 
