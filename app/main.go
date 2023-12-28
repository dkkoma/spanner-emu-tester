package main

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, "projects/test/instances/instance0/databases/database0")
	defer client.Close()
	if err != nil {
		panic(err)
	}
	funcs := []func(context.Context, *spanner.Client, int){query, insert, update}
	times := make([]time.Duration, 0)
	for _, f := range funcs {
		fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		fmt.Printf("----- %s -----\n", fname)
		for i := 1; i < 100; i++ {
			t := time.Now()
			f(ctx, client, i)
			time := time.Since(t)
			// fmt.Println(time)
			times = append(times, time)
		}
		fmt.Printf("----- %s average time: %v\n", fname, average(times))
	}
}

func average(times []time.Duration) time.Duration {
	var sum time.Duration
	for _, v := range times {
		sum += v
	}
	return sum / time.Duration(len(times))
}

func update(ctx context.Context, client *spanner.Client, i int) {
	stmt := spanner.Statement{SQL: `UPDATE table SET name = @name WHERE id = @id`}
	stmt.Params = make(map[string]interface{})
	stmt.Params["id"] = i
	stmt.Params["name"] = fmt.Sprintf("name%d", i)
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		return err
	})

	if err != nil {
		panic(err)
	}
}

func insert(ctx context.Context, client *spanner.Client, i int) {
	stmt := spanner.Statement{SQL: `INSERT INTO table (id, name, createdAt) VALUES (@id, @name, @createdAt)`}
	stmt.Params = make(map[string]interface{})
	stmt.Params["id"] = i
	stmt.Params["name"] = fmt.Sprintf("name%d", i)
	stmt.Params["createdAt"] = time.Now()
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		return err
	})

	if err != nil {
		panic(err)
	}
}

func query(ctx context.Context, client *spanner.Client, i int) {
	stmt := spanner.Statement{SQL: `SELECT count(1) FROM table`}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
	}
}
