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
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
)

var (
	serviceAccount   = os.Getenv("GOOGLE_SERVICE_ACCOUNT") // eg, export GOOGLE_SERVICE_ACCOUNT = "path to service account file"
	projectID        = os.Getenv("GOOGLE_PROJECT_ID")      // eg, export GOOGLE_PROJECT_ID ="conduit-connectors"
	datasetID        = "conduit_test_dataset"
	tableID          = "conduit_test_table"
	tableIDTimeStamp = "conduit_test_table_time_stamp"
	location         = "US"
)

// Initial setup required - project with service account.
func dataSetup(t *testing.T) (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return err
	}
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference("gs://cloud-samples-data/bigquery/us-states/us-states.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.Schema = bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "post_abbr", Type: bigquery.StringFieldType},
	}
	loader := client.Dataset(datasetID).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteEmpty

	job, err := loader.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if status.Err() != nil && !strings.Contains(status.Err().Error(), "duplicate") {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}

	t.Log("Table created:", tableID)

	return nil
}

// Item represents a row item.
type Item struct {
	Name      string
	Age       int
	Updatedat time.Time
}

// Initial setup required - project with service account.
func dataSetupWithTimestamp(t *testing.T) (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		t.Log("Dataset could not be created created")
		return err
	}

	sampleSchema := bigquery.Schema{
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "age", Type: bigquery.IntegerFieldType},
		{Name: "updatedat", Type: bigquery.TimestampFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableIDTimeStamp)
	err = tableRef.Create(ctx, metaData)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		t.Log("Error: ", err)
		return err
	}

	var query string

	for i := 0; i < 8; i++ {
		iString := fmt.Sprintf("%d", i)
		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableIDTimeStamp + "`  values ('name" + iString +
			"', " + iString + ", '" + time.Now().UTC().AddDate(0, 0, -i).Format("2006-01-02 15:04:05.999999 MST") + "' )"

		q := client.Query(query)
		q.Location = location

		job, err := q.Run(ctx)
		if err != nil {
			t.Log("Error found: ", err)
			return err
		}

		status, err := job.Wait(ctx)
		if err != nil {
			t.Log("Error found: ", err)
			return err
		}

		if err = status.Err(); err != nil {
			t.Log("Error found: ", err)
			return err
		}
	}

	return nil
}

func dataUpdationWithTimestamp(t *testing.T) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		t.Log("Error found: ", err)
	}
	defer client.Close()

	query := "UPDATE `" + projectID + "." + datasetID + "." + tableIDTimeStamp + "` " +
		" SET updatedat='" + time.Now().UTC().AddDate(0, 0, +10).Format("2006-01-02 15:04:05.999999 MST") + "' WHERE name =" + "'name4';"

	q := client.Query(query)
	q.Location = location

	job, err := q.Run(ctx)
	if err != nil {
		t.Log("Error found: ", err)
		return
	}

	status, err := job.Wait(ctx)
	if err != nil {
		t.Log("Error found: ", err)
		return
	}

	if err := status.Err(); err != nil {
		t.Log("Error found: ", err)
		return
	}
}

func cleanupDataset(t *testing.T, tables []string) (err error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	for _, tableID := range tables {
		table := client.Dataset(datasetID).Table(tableID)
		err := table.Delete(ctx)
		if err != nil && strings.Contains(err.Error(), "Not found") {
			return err
		}
	}

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsJSON([]byte(serviceAccount)))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	if err = client.Dataset(datasetID).Delete(ctx); err != nil {
		// dataset could already be in use. it is okay if it does not get deleted
		log.Println("Error in delete: ", err)
		return err
	}
	return err
}

func TestSuccessTimeIncremental(t *testing.T) {
	err := dataSetupWithTimestamp(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableIDTimeStamp})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: "updatedat",
		googlebigquery.ConfigPrimaryKeyColName:  "age",
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Log(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("error: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			t.Log("Error: ", err)
			break
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessTimeIncrementalAndUpdate(t *testing.T) {
	err := dataSetupWithTimestamp(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableIDTimeStamp})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: "updatedat",
		googlebigquery.ConfigPrimaryKeyColName:  "age",
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Log(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("errror: ", err)
	}

	var recordPos []byte

	time.Sleep(15 * time.Second)
	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			t.Log(err)
			break
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	// check updated
	dataUpdationWithTimestamp(t)

	err = src.Open(ctx, recordPos)
	if err != nil {
		t.Log("error: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		_, err = src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			t.Log(err)
			break
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessPrimaryKey(t *testing.T) {
	err := dataSetupWithTimestamp(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableIDTimeStamp})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableIDTimeStamp,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: "age",
		googlebigquery.ConfigPrimaryKeyColName:  "updatedat",
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		_, err = src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			t.Log(err)
			break
		}
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGetFromPosition(t *testing.T) {
	err := dataSetup(t)
	if err != nil {
		t.Log("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:    serviceAccount,
		googlebigquery.ConfigProjectID:         projectID,
		googlebigquery.ConfigDatasetID:         datasetID,
		googlebigquery.ConfigTableID:           tableID,
		googlebigquery.ConfigLocation:          location,
		googlebigquery.ConfigPrimaryKeyColName: "post_abbr",
	}
	googlebigquery.PollingTime = time.Second * 1

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Log(err)
	}
	position := "46"
	pos, err := json.Marshal(&position)
	if err != nil {
		t.Log(err)
	}

	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("error: ", err)
	}
	time.Sleep(15 * time.Second)
	for i := 0; i <= 4; i++ {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			t.Log(err)
			break
		}
		if err != nil || ctx.Err() != nil {
			t.Errorf("expected no error, got %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGetWholeDataset(t *testing.T) {
	err := dataSetup(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            tableID, // tableID,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigIncrementalColName: "post_abbr",
		googlebigquery.ConfigPrimaryKeyColName:  "post_abbr"}

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Log(err)
		t.Errorf("some other error found: %v", err)
	}

	googlebigquery.PollingTime = time.Second * 1
	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("errror: ", err)
		t.Errorf("some other error found: %v", err)
	}
	time.Sleep(10 * time.Second)

	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			break
		}
		if err != nil {
			t.Errorf("some other error found: %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulOrderByName(t *testing.T) {
	err := dataSetup(t)
	if err != nil {
		t.Errorf("Could not create values. Err: %v", err)
		return
	}
	defer func() {
		err := cleanupDataset(t, []string{tableID})
		if err != nil {
			t.Log("Got error while cleanup. Err: ", err)
		}
	}()

	src := Source{}
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigTableID:            tableID,
		googlebigquery.ConfigIncrementalColName: "post_abbr",
		googlebigquery.ConfigPrimaryKeyColName:  "post_abbr",
	}

	ctx := context.Background()
	err = src.Configure(ctx, cfg)
	if err != nil {
		t.Log(err)
		t.Errorf("some other error found: %v", err)
	}

	googlebigquery.PollingTime = time.Second * 1
	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		t.Log("errror: ", err)
		t.Errorf("some other error found: %v", err)
	}
	time.Sleep(10 * time.Second)

	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			break
		}
		if err != nil {
			t.Errorf("some other error found: %v", err)
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
