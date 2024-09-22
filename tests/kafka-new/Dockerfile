# Use the official OpenJDK image as the base image
FROM openjdk:17-jdk-slim

# Set environment variables for Kafka
ENV KAFKA_VERSION=3.7.1 \
    SCALA_VERSION=2.13 \
    KAFKA_HOME=/opt/kafka

# Install required packages and download Kafka
RUN apt-get update && \
    apt-get install -y wget netcat openssl && \
    wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -O /tmp/kafka.tgz && \
    mkdir -p ${KAFKA_HOME} && \
    tar -xzf /tmp/kafka.tgz --strip-components=1 -C ${KAFKA_HOME} && \
    rm -rf /tmp/kafka.tgz && \
    apt-get remove -y wget && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Add Kafka binaries to PATH
ENV PATH=${PATH}:${KAFKA_HOME}/bin

# Expose Kafka port (SSL)
EXPOSE 9093

# Set the working directory
WORKDIR ${KAFKA_HOME}

# Copy the startup script into the container
COPY start-kafka.sh /usr/bin/start-kafka.sh
RUN chmod +x /usr/bin/start-kafka.sh

# Copy the keystore and truststore
COPY secrets/kafka.broker.keystore.jks /opt/kafka/secrets/kafka.broker.keystore.jks
COPY secrets/kafka.broker.truststore.jks /opt/kafka/secrets/kafka.broker.truststore.jks
COPY ssl.properties /opt/kafka/config/ssl.properties

# Start Kafka when the container launches
CMD ["start-kafka.sh"]