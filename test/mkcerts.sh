#!/usr/bin/env bash

set -exo pipefail

SELF=$(readlink -f "$0")
DIR=$(dirname "$SELF")

cd "$DIR"

# https://gist.github.com/pcan/e384fcad2a83e3ce20f9a4c33f4a13ae

# Generate a Certificate Authority
openssl req -nodes -new -x509 -days 11499 -keyout ca-key.pem -out ca-crt.pem -subj "/C=AT/CN=localhost/O=Test CA Org/emailAddress=tester@example.com"

# Generate Server Key
openssl genrsa -out server-key.pem 4096

# Generate Server certificate signing request
openssl req -nodes -new -key server-key.pem -out server-csr.pem -subj "/C=AT/CN=localhost/O=Test Server Org/emailAddress=tester@example.com"

# Sign certificate using the CA
openssl x509 -req -days 11499 -in server-csr.pem -CA ca-crt.pem -CAkey ca-key.pem -CAcreateserial -out server-crt.pem

# Verify server certificate
openssl verify -CAfile ca-crt.pem server-crt.pem

# Generate Client Key
openssl genrsa -out client-key.pem 4096

# Generate Client certificate signing request
openssl req -nodes -new -key client-key.pem -out client-csr.pem -subj "/C=AT/CN=localhost/O=Test Client Org/emailAddress=tester@example.com"

# Sign certificate using the CA
openssl x509 -req -days 11499 -in client-csr.pem -CA ca-crt.pem -CAkey ca-key.pem -CAcreateserial -out client-crt.pem

# Verify client certificate
openssl verify -CAfile ca-crt.pem client-crt.pem
