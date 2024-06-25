// Package main implements the tool.
package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
)

func main() {

	//Determine Contact Point
	awsRegion := "us-east-1"
	contactPoint := fmt.Sprintf("cassandra.%s.amazonaws.com", awsRegion)
	fmt.Println("Using Contact Point ", contactPoint)

	// Configure Cluster
	cluster := gocql.NewCluster(contactPoint)

	cluster.Port = 9142
	cluster.NumConns = 4

	awsAuth := sigv4.NewAwsAuthenticator()
	cluster.Authenticator = awsAuth

	//Retry Policy
	amazonKeyspacesRetry := &AmazonKeyspacesExponentialBackoffRetryPolicy{Max: 100 * time.Millisecond, Min: 10 * time.Millisecond, NumRetries: 20}
	cluster.RetryPolicy = amazonKeyspacesRetry

	amazonKeyspacesConnectionObserver, _ := newAmazonKeyspacesObserver()
	cluster.ConnectObserver = amazonKeyspacesConnectionObserver

	// Configure Connection TrustStore for TLS
	cluster.SslOpts = &gocql.SslOptions{
		CaPath:                 "certs/sf-class2-root.crt",
		EnableHostVerification: false,
	}

	cluster.Consistency = gocql.LocalQuorum
	cluster.DisableInitialHostLookup = false

	cassandraSession, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Cassandra Session Creation Error: %v", err)
	}

	defer cassandraSession.Close()

	// Perform Query
	var keyspaceName string
	query := cassandraSession.Query("SELECT keyspace_name FROM system_schema.keyspaces;")
	query.Idempotent(true)
	iter := query.Iter()

	defer iter.Close()

	for iter.Scan(&keyspaceName) {
		fmt.Println("keyspace_name : ", keyspaceName)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

// newAmazonKeyspacesObserver creates observer for debugging connection.
func newAmazonKeyspacesObserver() (*amazonKeyspacesObserver, error) {
	s := &amazonKeyspacesObserver{}
	return s, nil
}

type amazonKeyspacesObserver struct{}

func (e *amazonKeyspacesObserver) ObserveConnect(q gocql.ObservedConnect) {
	if q.Err != nil {
		fmt.Printf("Error Connecting to IP:%s, Port:%d Error: %s\n", q.Host.ConnectAddress(), q.Host.Port(), q.Err)
	} else {
		fmt.Printf("Connected to hostid:%s, IP:%s, Port:%d, elapse:%d\n", q.Host.HostID(), q.Host.ConnectAddress(), q.Host.Port(), q.End.UnixMilli()-q.Start.UnixMilli())
	}
}

// AmazonKeyspacesExponentialBackoffRetryPolicy will retry exponentially on the same connection
type AmazonKeyspacesExponentialBackoffRetryPolicy struct {
	NumRetries int
	Min, Max   time.Duration
}

// Attempt implements gocql.RetryPolicy.
func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) Attempt(q gocql.RetryableQuery) bool {
	if q.Attempts() > e.NumRetries {
		return false
	}
	time.Sleep(e.napTime(q.Attempts()))
	return true
}

// used to calculate exponentially growing time
func getExponentialTime(min time.Duration, max time.Duration, attempts int) time.Duration {
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 10 * time.Second
	}
	minFloat := float64(min)
	napDuration := minFloat * math.Pow(2, float64(attempts-1))
	// add some jitter
	napDuration += rand.Float64()*minFloat - (minFloat / 2)
	if napDuration > float64(max) {
		return time.Duration(max)
	}
	return time.Duration(napDuration)
}

// GetRetryType will always retry instead of RetryNextHost.
func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) GetRetryType(_ error) gocql.RetryType {
	return gocql.Retry
}
func (e *AmazonKeyspacesExponentialBackoffRetryPolicy) napTime(attempts int) time.Duration {
	return getExponentialTime(e.Min, e.Max, attempts)
}
