package synapsys_test

import (
	"context"
	"fmt"
	"time"

	"github.com/basamento/synapsys-go-sdk"
)

func ExampleWorker_Register() {
	worker, err := synapsys.New(synapsys.WithEnabled(false))
	if err != nil {
		panic(err)
	}
	err = worker.Register(
		synapsys.EndlessContext("listener", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		synapsys.Progressive("report", func() error { return nil }),
	)
	if err != nil {
		panic(err)
	}

	for _, process := range worker.Processes() {
		fmt.Println(process.Name, process.Type, process.State)
	}
	// Output:
	// listener endless idle
	// report progressive idle
}

func ExampleWorker_Stop() {
	worker, err := synapsys.New(synapsys.WithEnabled(false))
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = worker.Stop(ctx)
	// Output:
}
