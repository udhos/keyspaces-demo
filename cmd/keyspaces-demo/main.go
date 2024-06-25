// Package main implements the tool.
package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
)

func main() {
	// configuring the cluster options
	region := "us-east-1"
	host := fmt.Sprintf("cassandra.%s.amazonaws.com:9142", region)
	cluster := gocql.NewCluster(host)
	var auth sigv4.AwsAuthenticator = sigv4.NewAwsAuthenticator()
	auth.Region = region
	/*
		auth.Region = region
		auth.AccessKeyId = "AKIAIOSFODNN7EXAMPLE"
		auth.SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	*/

	cluster.Authenticator = auth

	cluster.SslOpts = &gocql.SslOptions{
		//CaPath: "certs/AmazonRootCA1.pem",
	}
	cluster.Consistency = gocql.LocalQuorum
	cluster.DisableInitialHostLookup = true

	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("err>", err)
		return
	}
	defer session.Close()

	// doing the query
	var text string
	iter := session.Query("SELECT keyspace_name FROM system_schema.tables;").Iter()
	for iter.Scan(&text) {
		fmt.Println("keyspace_name:", text)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}
