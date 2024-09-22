#!/bin/bash

# Start Zookeeper in the background
${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties &

# Wait for Zookeeper to initialize
echo "Waiting for Zookeeper to start..."
while ! nc -z localhost 2181; do
  sleep 1
done
echo "Zookeeper started."

# Set default passwords if not provided
KEYSTORE_PASSWORD=${KEYSTORE_PASSWORD:-brokerpass}
TRUSTSTORE_PASSWORD=${TRUSTSTORE_PASSWORD:-brokerpass}
KEY_PASSWORD=${KEY_PASSWORD:-brokerpass}

# Configure SSL settings in server.properties
echo "Configuring SSL settings in server.properties..."

cat <<EOL >> ${KAFKA_HOME}/config/server.properties

############################# SSL Configuration #############################
listeners=SSL://:9093
advertised.listeners=SSL://localhost:9093
security.inter.broker.protocol=SSL
ssl.keystore.location=/opt/kafka/secrets/kafka.broker.keystore.jks
ssl.keystore.password=brokerpass
ssl.key.password=brokerpass
ssl.truststore.location=/opt/kafka/secrets/kafka.broker.truststore.jks
ssl.truststore.password=brokerpass
ssl.client.auth=required
EOL

# Start Kafka server in the background
${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties &

# Wait for Kafka to initialize
echo "Waiting for Kafka broker to start..."
while ! nc -z localhost 9093; do
  sleep 1
done
echo "Kafka broker started."

# Create the topic 'kafka-rust-test' if it doesn't exist
echo "Creating topic 'kafka-rust-test'..."
${KAFKA_HOME}/bin/kafka-topics.sh --create --if-not-exists \
  --topic kafka-rust-test \
  --bootstrap-server localhost:9093 \
  --replication-factor 1 \
  --partitions 1 \
  --command-config ${KAFKA_HOME}/config/ssl.properties
echo "Topic 'kafka-rust-test' created."

# Keep the script running to prevent the container from exiting
wait