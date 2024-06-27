// Package main implements the tool.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
	"github.com/segmentio/ksuid"
)

func getRegion() string {
	if r := os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
		return r
	}
	return "us-east-1"
}

func getHost(region string) string {
	if h := os.Getenv("ENDPOINT"); h != "" {
		return h
	}

	return fmt.Sprintf("cassandra.%s.amazonaws.com:9142", region)
}

func main() {
	// configuring the cluster options
	region := getRegion()
	host := getHost(region)
	log.Printf("host: %s", host)

	cluster := gocql.NewCluster(host)
	var auth sigv4.AwsAuthenticator = sigv4.NewAwsAuthenticator()
	auth.Region = region
	/*
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
		log.Fatalf("cluster session: %v", err)
	}
	defer session.Close()

	// doing the query

	ctx := context.Background()

	query1 := envString("QUERY1", "select * from system.peers")
	err = printQuery(ctx, "query1", true, true, session, query1)
	if err != nil {
		log.Fatal(err)
	}

	query2 := envString("QUERY2", "SELECT keyspace_name, table_name FROM system_schema.tables;")
	err = printQuery(ctx, "query2", true, true, session, query2)
	if err != nil {
		log.Fatal(err)
	}

	concurrency := envInt64("INSERT_CONCURRENCY", 1)

	var wg sync.WaitGroup

	for range concurrency {
		wg.Add(1)
		go func() {
			insert(session)
			wg.Done()
		}()
	}

	wg.Wait()

	query3 := envString("QUERY3", "select * from demo.demo1")
	query3print := envBool("QUERY3_PRINT", false)
	err = printQuery(ctx, "query3", query3print, true, session, query3)
	if err != nil {
		log.Fatal(err)
	}

}

func insert(session *gocql.Session) {
	insertLimit := envInt64("INSERT_LIMIT", 1)
	insertDuration := envDuration("INSERT_DURATION", time.Second)

	const batchSize = 30

	begin := time.Now()

	var countInsert int64

	ctx := context.Background()

	b := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

	for range insertLimit {
		i, errUID := ksuid.NewRandom()
		if errUID != nil {
			log.Fatalf("uid error: %v", errUID)
		}
		id := i.String()

		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt:       "insert into demo.demo1 (id, name) values (?,?)",
			Args:       []interface{}{id, id},
			Idempotent: true,
		})

		countInsert++

		if len(b.Entries) >= batchSize {
			if err := session.ExecuteBatch(b); err != nil {
				log.Fatalf("execute batch error: %v", err)
			}
			b = session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
			log.Printf("inserted: count=%d elapsed=%v", countInsert, time.Since(begin))
		}

		if time.Since(begin) >= insertDuration {
			break
		}
	}

	if len(b.Entries) > 0 {
		if err := session.ExecuteBatch(b); err != nil {
			log.Fatalf("execute batch error: %v", err)
		}
	}

	log.Printf("inserted: count=%d elapsed=%v", countInsert, time.Since(begin))
}

func envString(envVar, defaultValue string) string {
	value := defaultValue
	q := os.Getenv(envVar)
	if q != "" {
		value = q
	}
	log.Printf("%s='%s' default='%s' value='%s'", envVar, q, defaultValue, value)
	return value
}

func envBool(envVar string, defaultValue bool) bool {
	value := defaultValue
	q := os.Getenv(envVar)
	if q != "" {
		var errConv error
		value, errConv = strconv.ParseBool(q)
		if errConv != nil {
			log.Printf("envBool: %s: error: %v", envVar, errConv)
		}
	}
	log.Printf("%s='%s' default=%t value=%t", envVar, q, defaultValue, value)
	return value
}

func envInt64(envVar string, defaultValue int64) int64 {
	value := defaultValue
	q := os.Getenv(envVar)
	if q != "" {
		var errConv error
		value, errConv = strconv.ParseInt(q, 10, 64)
		if errConv != nil {
			log.Printf("envInt64: %s: error: %v", envVar, errConv)
		}
	}
	log.Printf("%s='%s' default=%d value=%d", envVar, q, defaultValue, value)
	return value
}

func envDuration(envVar string, defaultValue time.Duration) time.Duration {
	value := defaultValue
	q := os.Getenv(envVar)
	if q != "" {
		var errConv error
		value, errConv = time.ParseDuration(q)
		if errConv != nil {
			log.Printf("envDuration: %s: error: %v", envVar, errConv)
		}
	}
	log.Printf("%s='%s' default=%v value=%v", envVar, q, defaultValue, value)
	return value
}

func printQuery(ctx context.Context, label string, print, stats bool, session *gocql.Session, stmt string, values ...interface{}) error {

	var rows int
	begin := time.Now()

	if stats {
		defer func() {
			log.Printf("%s: printQuery: rows=%d elapsed=%v", label, rows, time.Since(begin))
		}()
	}

	iter := session.Query(stmt, values...).WithContext(ctx).Iter()
	if print {
		fmt.Println("**", stmt)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ',
		0)
	for i, columnInfo := range iter.Columns() {
		if !print {
			continue
		}
		if i > 0 {
			fmt.Fprint(w, "\t| ")
		}
		fmt.Fprintf(w, "%s (%s)", columnInfo.Name, columnInfo.TypeInfo)
	}

	for {
		rd, err := iter.RowData()
		if err != nil {
			return err
		}
		if !iter.Scan(rd.Values...) {
			break
		}
		if print {
			fmt.Fprint(w, "\n")
		}
		for i, val := range rd.Values {
			if !print {
				continue
			}
			if i > 0 {
				fmt.Fprint(w, "\t| ")
			}

			fmt.Fprint(w, reflect.Indirect(reflect.ValueOf(val)).Interface())
		}
		rows++
	}

	if print {
		fmt.Fprint(w, "\n")
		w.Flush()
		fmt.Println()
	}

	return iter.Close()
}
