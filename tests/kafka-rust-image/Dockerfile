FROM ubuntu:xenial

RUN apt-get update && apt-get install -y curl jq coreutils net-tools openjdk-8-jre

ARG kafka_version=1.0.0
ARG scala_version=2.12

ENV KAFKA_VERSION=$kafka_version SCALA_VERSION=$scala_version
#ENV KAFKA_ZOOKEEPER_CONNECT zookeeper

COPY download-kafka.sh /tmp/download-kafka.sh

RUN /tmp/download-kafka.sh && \
    tar xfz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    rm /tmp/download-kafka.sh

RUN mkdir -p /kafka/logs

ENV KAFKA_HOME /opt/kafka
ENV PATH ${PATH}:${KAFKA_HOME}/bin

COPY start-kafka.sh /usr/bin/start-kafka.sh
COPY create-topics.sh /usr/bin/create-topics.sh

ENV KAFKA_ZOOKEEPER_CONNECT zookeeper
COPY server.properties secure.server.properties /opt/kafka/config/

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

EXPOSE 9092

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["start-kafka.sh"]
