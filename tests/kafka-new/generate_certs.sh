#!/bin/bash

# Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -sha256 -days 3650 -out ca.crt -subj "/CN=Kafka-CA"

# Generate broker key and certificate
openssl genrsa -out kafka.broker.key 2048
openssl req -new -key kafka.broker.key -out kafka.broker.csr -subj "/CN=localhost"
openssl x509 -req -in kafka.broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out kafka.broker.crt -days 365 -sha256

# Create keystore and truststore
# ... (rest of the commands)

# Create a PKCS12 keystore
openssl pkcs12 -export -in kafka.broker.crt -inkey kafka.broker.key -chain -CAfile ca.crt -name kafka-broker -out kafka.broker.p12 -password pass:brokerpass

# Convert PKCS12 keystore to JKS format
keytool -importkeystore -destkeystore kafka.broker.keystore.jks -srckeystore kafka.broker.p12 -srcstoretype PKCS12 -alias kafka-broker -storepass brokerpass -srcstorepass brokerpass

# Create truststore and import the CA certificate
keytool -keystore kafka.broker.truststore.jks -alias CARoot -import -file ca.crt -storepass brokerpass -noprompt

mkdir secrets
mv kafka.broker.truststore.jks secrets/
mv kafka.broker.keystore.jks secrets/

rm ca.crt
rm ca.key
rm kafka.broker.crt
rm kafka.broker.csr
rm kafka.broker.key
rm kafka.broker.p12