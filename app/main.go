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
	tables := []string{"table1", "table2"}
	funcs := []func(context.Context, *spanner.Client, string, int){query, insert, update, query}
	for _, table := range tables {
		fmt.Printf("===== %s =====\n", table)
		for _, f := range funcs {
			times := make([]time.Duration, 0)
			fname := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
			fmt.Printf("----- %s -----\n", fname)
			for i := 1; i < 100; i++ {
				t := time.Now()
				f(ctx, client, table, i)
				time := time.Since(t)
				// fmt.Println(time)
				times = append(times, time)
			}
			fmt.Printf("----- %s average time: %v\n", fname, average(times))
		}
	}
}

func average(times []time.Duration) time.Duration {
	var sum time.Duration
	for _, v := range times {
		sum += v
	}
	return sum / time.Duration(len(times))
}

func update(ctx context.Context, client *spanner.Client, table string, i int) {
	stmt := spanner.Statement{SQL: fmt.Sprintf(`UPDATE %s SET name = @name WHERE id = @id`, table)}
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

func insert(ctx context.Context, client *spanner.Client, table string, i int) {
	stmt := spanner.Statement{SQL: fmt.Sprintf(`INSERT INTO %s (id, name, createdAt) VALUES (@id, @name, @createdAt)`, table)}
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

func query(ctx context.Context, client *spanner.Client, table string, i int) {
	stmt := spanner.Statement{SQL: fmt.Sprintf(`SELECT * FROM %s WHERE id = @id`, table)}
	stmt.Params = make(map[string]interface{})
	stmt.Params["id"] = i
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
