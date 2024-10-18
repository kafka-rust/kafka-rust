FROM ubuntu:latest

RUN apt-get update && apt-get install -y wget jq coreutils net-tools openjdk-8-jre

ARG kafka_version
ARG scala_version

ENV KAFKA_VERSION=$kafka_version SCALA_VERSION=$scala_version
#ENV KAFKA_ZOOKEEPER_CONNECT zookeeper

RUN export KAFKA_TGZ="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    && wget -q ${KAFKA_TGZ} -O - | tar xz -C /opt \
    && ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

RUN mkdir -p /kafka/logs

ENV KAFKA_HOME /opt/kafka
ENV PATH ${PATH}:${KAFKA_HOME}/bin

COPY start-kafka.sh /usr/bin/start-kafka.sh
COPY create-topics.sh /usr/bin/create-topics.sh

ENV KAFKA_ZOOKEEPER_CONNECT zookeeper
COPY server.properties secure.server.properties client-ssl.properties /opt/kafka/config/

# set up an SSL certificate for secure mode
RUN keytool \
        -keystore server.keystore.jks \
        -alias localhost \
        -validity 1 \
        -genkeypair \
        -keyalg RSA \
        -dname "CN=Unknown, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" \
        -storepass xxxxxx \
        -keypass xxxxxx

RUN keytool -exportcert -alias localhost -keystore /server.keystore.jks -file server-cert.pem -storepass xxxxxx -noprompt 
RUN keytool -importcert -file server-cert.pem -alias server-cert -keystore client.truststore.jks -storepass xxxxxx -noprompt
RUN keytool -list -v -keystore client.truststore.jks -storepass xxxxxx -noprompt

EXPOSE 9092

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["start-kafka.sh"]
