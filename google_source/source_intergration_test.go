package googlesource

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
)

func TestConfigureSource_FailsWhenConfigEmpty(t *testing.T) {
	con := Source{}
	err := con.Configure(context.Background(), make(map[string]string))
	if err == nil {
		t.Errorf("expected no error, got %v", err)

	}

	if strings.HasPrefix(err.Error(), "config is invalid:") {
		t.Errorf("expected error to be about missing config, got %v", err)
	}
}

func TestSuccessfulGet(t *testing.T) {

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json"
	cfg[googlebigquery.ConfigProjectID] = "conduit-connectors"
	cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	cfg[googlebigquery.ConfigTableID] = "test1"
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	for { //i := 0; i <= 4; i++
		fmt.Println("Calling read again....")
		record, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}
		if err != nil {
			t.Errorf("some other game found: %v", err)
		}
		value := string(record.Payload.Bytes())
		fmt.Println("Record found:", value)
	}
	fmt.Println("Calling teardown for start...")
	err = src.Teardown(ctx)
	fmt.Println("Calling teardown...")
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}

}

func TestSuccessfulGetWholeDataset(t *testing.T) {

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json"
	cfg[googlebigquery.ConfigProjectID] = "conduit-connectors"
	cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	// cfg[googlebigquery.ConfigTableID] = "test1"
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	for i := 0; i <= 4; i++ {
		fmt.Println("Calling read again....")
		record, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}

		value := string(record.Payload.Bytes())
		fmt.Println("Record found:", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}

func TestSuccessfulTearDown(t *testing.T) {

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "/home/nehagupta/Downloads/conduit-connectors-cf3466b16662.json"
	cfg[googlebigquery.ConfigProjectID] = "conduit-connectors"
	cfg[googlebigquery.ConfigDatasetID] = "conduit_1"
	cfg[googlebigquery.ConfigTableID] = "test1"
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		fmt.Println(err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)

	}
}
