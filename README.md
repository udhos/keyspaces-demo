# keyspaces-demo

# Quick start

    git clone https://github.com/udhos/keyspaces-demo
    cd keyspaces-demo
    go run ./cmd/keyspaces-demo

# Full build

    git clone https://github.com/udhos/keyspaces-demo
    cd keyspaces-demo
    ./build.sh
    keyspaces-demo

# Source

Example from: https://github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin

## gocql auth plugin

https://github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin

This package implements an authentication plugin for the open-source Gocql Driver for Apache Cassandra.

## SSL 

Amazon Keyspaces requires the use of Transport Layer Security (TLS) to help secure connections with clients. To connect to Amazon Keyspaces using TLS, you need to download an Amazon digital certificate and configure the Go driver to use TLS.

Download the Starfield digital certificate using the following command and save sf-class2-root.crt locally or in your home directory.

    curl https://certs.secureserver.net/repository/sf-class2-root.crt

    curl https://www.amazontrust.com/repository/AmazonRootCA1.pem