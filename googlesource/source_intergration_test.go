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
	"bytes"
	"context"
	"encoding/gob"
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
	tableID2         = "conduit_test_table_2"
	tableIDTimeStamp = "conduit_test_table_time_stamp"
	location         = "US"
	globalCounter    = 0
)

func DataSetup() (err error) {
	err = dataSetup()
	if err != nil {
		return err
	}
	return err
}

// Initial setup required - project with service account.
func dataSetup() (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
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
	fmt.Println("Dataset created")
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
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

	fmt.Println("Table created:", tableID)

	return nil
}

// dataSetupWithRecord Initial setup required - project with service account.
func dataSetupWithRecord(config map[string]string, record []sdk.Record) (result []sdk.Record, err error) {
	ctx := context.Background()
	tableID := config[googlebigquery.ConfigTableID]

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	meta := &bigquery.DatasetMetadata{
		Location: location, // See https://cloud.google.com/bigquery/docs/locations
	}

	// create dataset
	if err := client.Dataset(datasetID).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "duplicate") {
		return result, err
	}
	fmt.Println("Dataset created")
	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return result, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	sampleSchema := bigquery.Schema{
		{Name: "abb", Type: bigquery.StringFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
	}

	metaData := &bigquery.TableMetadata{
		Schema:         sampleSchema,
		ExpirationTime: time.Now().AddDate(1, 0, 0), // Table will be automatically deleted in 1 year.
	}
	tableRef := client.Dataset(datasetID).Table(tableID)
	err = tableRef.Create(ctx, metaData)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		return result, err
	}

	var query string
	positions := make(map[string]string)

	for i := 0; i < len(record); i++ {
		// name := fmt.Sprintf("%s", record[i].Payload.Bytes())
		name := fmt.Sprintf("name%v", time.Now().AddDate(0, 0, globalCounter).Format("20060102150405"))
		abb := fmt.Sprintf("name%v", time.Now().AddDate(0, 0, globalCounter).Format("20060102150405"))
		query = "INSERT INTO `" + projectID + "." + datasetID + "." + tableID + "`  values ('" + abb + "' , '" + name + "')"

		data := make(sdk.StructuredData)
		data["abb"] = abb
		data["name"] = name

		key := name

		buffer := &bytes.Buffer{}
		if err := gob.NewEncoder(buffer).Encode(key); err != nil {
			return result, err
		}
		byteKey := buffer.Bytes()

		positions[tableID] = fmt.Sprintf("'%s'", name)
		positionRecord, err := json.Marshal(&positions)
		if err != nil {
			log.Println("error found", err)
			return result, err
		}

		result = append(result, sdk.Record{Payload: data, Key: sdk.RawData(byteKey), Position: positionRecord})
		q := client.Query(query)
		q.Location = location

		job, err := q.Run(ctx)
		if err != nil {
			log.Println("Error found: ", err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			log.Println("Error found: ", err)
			return result, err
		}

		if err = status.Err(); err != nil {
			log.Println("Error found: ", err)
		}
		globalCounter++
	}
	return result, nil
}

// Item represents a row item.
type Item struct {
	Name      string
	Age       int
	Updatedat time.Time
}

// Initial setup required - project with service account.
func dataSetupWithTimestamp() (err error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
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
	fmt.Println("Dataset created")

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
	fmt.Println("Error: ", err)
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
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
			log.Println("Error found: ", err)
		}

		status, err := job.Wait(ctx)
		if err != nil {
			log.Println("Error found: ", err)
		}

		if err := status.Err(); err != nil {
			log.Println("Error found: ", err)
		}
	}

	return nil
}

func dataUpdationWithTimestamp() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		log.Println("Error found: ", err)
	}
	defer client.Close()

	query := "UPDATE `" + projectID + "." + datasetID + "." + tableIDTimeStamp + "` " +
		" SET updatedat='" + time.Now().UTC().AddDate(0, 0, +10).Format("2006-01-02 15:04:05.999999 MST") + "' WHERE name =" + "'name4';"

	q := client.Query(query)
	q.Location = location

	job, err := q.Run(ctx)
	if err != nil {
		log.Println("Error found: ", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Println("Error found: ", err)
	}

	if err := status.Err(); err != nil {
		log.Println("Error found: ", err)
	}
}

func cleanupDataset(tables []string) (err error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
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

	client, err = bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(serviceAccount))
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	if err = client.Dataset(datasetID).Delete(ctx); err != nil {
		fmt.Println("Error in delete: ", err)
		return err
	}
	return err
}

func TestSuccessTimeIncremental(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("error: ", err)
	}
	time.Sleep(5 * time.Second)
	for {
		_, err = src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessTimeIncrementalAndUpdate(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println(err)
	}
	pos := sdk.Position{}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("error: ", err)
	}

	var recordPos []byte

	time.Sleep(15 * time.Second)
	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}
	}

	// check updated
	dataUpdationWithTimestamp()

	err = src.Open(ctx, recordPos)
	if err != nil {
		fmt.Println("error: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		_, err = src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println(err)
			break
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessPrimaryKey(t *testing.T) {
	err := dataSetupWithTimestamp()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableIDTimeStamp})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for {
		_, err = src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGetFromPosition(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println(err)
	}
	position := "46"
	pos, err := json.Marshal(&position)
	if err != nil {
		fmt.Println(err)
	}

	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
	}
	time.Sleep(15 * time.Second)
	for i := 0; i <= 4; i++ {
		_, err := src.Read(ctx)
		if err != nil || ctx.Err() != nil {
			fmt.Println(err)
			break
		}
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestSuccessfulGetWholeDataset(t *testing.T) {
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println(err)
		t.Errorf("some other error found: %v", err)
	}

	googlebigquery.PollingTime = time.Second * 1
	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
		t.Errorf("some other error found: %v", err)
	}
	time.Sleep(10 * time.Second)

	for {
		_, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println("err: ", err)
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
	err := dataSetup()
	if err != nil {
		fmt.Println("Could not create values. Err: ", err)
		return
	}
	defer func() {
		err := cleanupDataset([]string{tableID})
		fmt.Println("Got error while cleanup. Err: ", err)
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
		fmt.Println(err)
		t.Errorf("some other error found: %v", err)
	}

	googlebigquery.PollingTime = time.Second * 1
	pos := sdk.Position{}
	err = src.Open(ctx, pos)
	if err != nil {
		fmt.Println("errror: ", err)
		t.Errorf("some other error found: %v", err)
	}
	time.Sleep(5 * time.Second)

	for {
		record, err := src.Read(ctx)
		if err != nil && err == sdk.ErrBackoffRetry {
			fmt.Println("err: ", err)
			break
		}
		if err != nil {
			t.Errorf("some other error found: %v", err)
		}
		value := string(record.Position)
		fmt.Println("Record found:", value)
		value = string(record.Payload.Bytes())
		fmt.Println(":", value)
	}

	err = src.Teardown(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
