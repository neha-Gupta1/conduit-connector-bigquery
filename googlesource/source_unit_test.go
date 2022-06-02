// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlesource

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
	"gopkg.in/tomb.v2"
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

func TestSuccessfulTearDown(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}

	defer func() {
		err := cleanupDataset()
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{}

	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = tableID
	cfg[googlebigquery.ConfigLocation] = location

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestMultipleTables(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset()
		fmt.Println("Got error while cleanup. Err: ", err)
	}()

	src := Source{}
	cfg := map[string]string{}

	googlebigquery.PollingTime = time.Second * 1
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = fmt.Sprintf("%v,%v", tableID, tableID2)
	cfg[googlebigquery.ConfigLocation] = location

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("error found: %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Errorf("error found: %v", err)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestInvalidCreds(t *testing.T) {
	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = "invalid"
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = tableID
	cfg[googlebigquery.ConfigLocation] = "test"

	googlebigquery.PollingTime = time.Second * 1
	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err == nil {
		fmt.Println("we should get error in read")
	}
	time.Sleep(10 * time.Second)
	for {
		_, err := src.Read(ctx)
		if err != nil {
			fmt.Println("got the expected error. Err: ", err)
			break
		}
		if err == nil {
			t.Errorf("We should have seen some error: %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestNewSource(t *testing.T) {
	NewSource()
}

func TestAck(t *testing.T) {
	s := NewSource()
	err := s.Ack(context.TODO(), sdk.Position{})
	if err != nil {
		t.Errorf("Got err: %v", err)
	}
}

func TestTableFetchInvalidCred(t *testing.T) {
	s := Source{}
	_, err := s.listTables("", "")
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestNextContextDone(t *testing.T) {
	s := Source{}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	s.tomb = &tomb.Tomb{}
	_, err := s.Next(ctx)
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestInvalid(t *testing.T) {
	googlebigquery.PollingTime = time.Second * 1
	tmpClient := newClient
	defer func() {
		newClient = tmpClient
	}()

	newClient = func(ctx context.Context, projectID string, opts ...option.ClientOption) (*bigquery.Client, error) {
		return nil, fmt.Errorf("mock")
	}

	src := Source{}
	cfg := map[string]string{}
	cfg[googlebigquery.ConfigServiceAccount] = serviceAccount
	cfg[googlebigquery.ConfigProjectID] = projectID
	cfg[googlebigquery.ConfigDatasetID] = datasetID
	cfg[googlebigquery.ConfigTableID] = tableID
	cfg[googlebigquery.ConfigLocation] = location

	ctx := context.Background()
	err := src.Configure(ctx, cfg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	pos, err := json.Marshal(Position{TableID: "conduit_test_table", Offset: 46})
	if err != nil {
		fmt.Println(err)
	}

	err = src.Open(ctx, pos)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	time.Sleep(15 * time.Second)
	_, err = src.Read(ctx)
	if err == nil {
		t.Errorf("should have recieved error while pulling data")
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
