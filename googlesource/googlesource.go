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
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/iterator"
)

var (
	newClient = bigquery.NewClient
)

type readRowInput struct {
	tableID string
	offset  int
	wg      *sync.WaitGroup
}

// haris: why does rowInput need to be a chan?
// Neha: the function is getting called inside a goroutine we get wrong value (everytime the last possible values) and
// func param will change for each function call
func (s *Source) ReadGoogleRow(rowInput readRowInput, responseCh chan sdk.Record) (err error) {

	input := rowInput
	offset := input.offset
	tableID := input.tableID
	wg := input.wg

	lastRow := false

	defer wg.Done()
	for {
		// Keep on reading till end of table
		sdk.Logger(s.ctx).Trace().Str("tableID", tableID).Msg("inside read google row infinite for loop")
		if lastRow {
			sdk.Logger(s.ctx).Trace().Str("tableID", tableID).Msg("Its the last row. Done processing table")
			break
		}

		counter := 0
		// iterator
		it, err := s.getRowIterator(offset, tableID)
		if err != nil {
			fmt.Println("Error: ", err)
			sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return err
		}

		for {
			var row []bigquery.Value
			// select statement to make sure channel was not closed by teardown stage
			select {
			case <-s.iteratorClosed:
				sdk.Logger(s.ctx).Trace().Msg("recieved closed channel")
				return nil
			default:
				sdk.Logger(s.ctx).Trace().Msg("iterator running")
			}

			err := it.Next(&row)
			Schema := it.Schema

			if err == iterator.Done {
				sdk.Logger(s.ctx).Trace().Str("counter", fmt.Sprintf("%d", counter)).Msg("iterator is done.")
				if counter < googlebigquery.CounterLimit {
					// if counter is smaller than the limit we have reached the end of
					// iterator. And will break the for loop now.
					lastRow = true
				}
				break
			}
			if err != nil {
				sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("error while iterating")
				return err
			}

			// haris: does BQ have its own way of tracking rows, i.e. its own offsets?
			// Neha: Could not find any. Tables metadata does not provide any such info.
			// Users generally have some keys to do so. And we are working on meta-data of
			// table and not actual data.
			offset++
			key := Position{
				TableID: tableID,
				Offset:  offset,
			}

			buffer := &bytes.Buffer{}
			gob.NewEncoder(buffer).Encode(key)
			byteSlice := buffer.Bytes()

			counter++

			// keep the track of last rows fetched for each table.
			// this helps in implementing incremental syncing.
			s.wrtieLatestPosition(key)
			pos := s.latestPositions.LatestPositions
			recPosition, err := json.Marshal(&pos)
			if err != nil {
				sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			data := make(sdk.StructuredData)
			for i, r := range row {
				data[Schema[i].Name] = r
			}

			record := sdk.Record{
				CreatedAt: time.Now().UTC(),
				Payload:   data,
				Key:       sdk.RawData(byteSlice),
				Position:  recPosition}

			responseCh <- record
		}
	}
	return
}

func (s *Source) wrtieLatestPosition(key Position) {

	if len(s.latestPositions.LatestPositions) == 0 {
		s.latestPositions.lock.Lock()
		s.latestPositions.LatestPositions = make(map[string]int)
		s.latestPositions.LatestPositions[key.TableID] = key.Offset
		s.latestPositions.lock.Unlock()
	} else {
		s.latestPositions.lock.Lock()
		s.latestPositions.LatestPositions[key.TableID] = key.Offset
		s.latestPositions.lock.Unlock()
	}

}

// getRowIterator sync data for bigquery using bigquery client jobs
// haris proposal to rename to getRowIterator, since it's not returning a single row
// Neha: DONE
func (s *Source) getRowIterator(offset int, tableID string) (it *bigquery.RowIterator, err error) {
	// haris: does BigQuery guarantee ordering?
	// Neha: DONE. it does not guarantee ordering and so have added a config where user can provide the column name which
	// would be used as orderBy value. Orderby is not mandatory though

	query := "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
		" LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + strconv.Itoa(offset)

	if orderby, ok := s.sourceConfig.Config.Orderby[tableID]; ok {
		query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
			"ORDER BY " + orderby + " LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + strconv.Itoa(offset)
	}
	q := s.bqReadClient.Query(query)
	sdk.Logger(s.ctx).Trace().Str("q ", q.Q)
	q.Location = s.sourceConfig.Config.Location

	job, err := q.Run(s.tomb.Context(s.ctx))
	if err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running the job")
		return it, err
	}

	status, err := job.Wait(s.tomb.Context(s.ctx))
	if err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	if err := status.Err(); err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	it, err = job.Read(s.tomb.Context(s.ctx))
	if err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}
	return it, err
}

// listTables demonstrates iterating through the collection of tables in a given dataset.
func (s *Source) listTables(projectID, datasetID string) ([]string, error) {
	ctx := context.Background()
	tables := []string{}

	ts := s.bqReadClient.Dataset(datasetID).Tables(ctx)
	for {
		t, err := ts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return []string{}, err
		}
		tables = append(tables, t.TableID)
	}
	return tables, nil
}

// Next returns the next record from the buffer.
func (s *Source) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-s.tomb.Dead():
		return sdk.Record{}, s.tomb.Err()
	case r := <-s.records:
		return r, nil
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}

func fetchPos(s *Source, pos sdk.Position) {

	var latestPositions map[string]int
	err := json.Unmarshal(pos, &latestPositions)
	if err != nil {
		sdk.Logger(s.ctx).Info().Msg("Could not get position. Will start with offset 0")
		// s.snapshot = true
	}
	fmt.Println("WIll make map")

	s.latestPositions.lock.Lock()
	s.latestPositions.LatestPositions = make(map[string]int)
	s.latestPositions.LatestPositions = latestPositions
	s.latestPositions.lock.Unlock()

	fmt.Printf("fasdkfhlmap %v", s.latestPositions.LatestPositions)

}

func getTables(s *Source) (err error) {
	if s.sourceConfig.Config.TableID == "" {
		s.tables, err = s.listTables(s.sourceConfig.Config.ProjectID, s.sourceConfig.Config.DatasetID)
		if err != nil {
			sdk.Logger(s.ctx).Trace().Str("err", err.Error()).Msg("error found while listing table")
		}
	} else {
		s.tables = strings.SplitAfter(s.sourceConfig.Config.TableID, ",")
	}
	return err
}

// split into more methods for readability
// Neha: DONE
func (s *Source) runIterator() (err error) {

	if err = getTables(s); err != nil {
		sdk.Logger(s.ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
		return err
	}

	fecthDataForTables(s)

	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()
		case <-s.ticker.C:
			sdk.Logger(s.ctx).Trace().Msg("ticker started ")
			// create new client everytime the new sync start. This make sure that new tables coming in are handled.
			// haris: can we list tables in a way which doesn't require us to create a new client every polling period?
			// in other words, why can't we list all the tables with an existing client?
			// I'm concerned about the time overhead but also about new connections.
			//Neha: DONE

			if err = getTables(s); err != nil {
				sdk.Logger(s.ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
				return err
			}

			// if its an already running pipeline and we just want to check for any new rows.
			// Send the offset as last position where we left.
			fecthDataForTables(s)

		}
	}
}

func fecthDataForTables(s *Source) {
	// wait group make sure that we start new iteration only
	//  after the first iteration is completely done.
	var rowInput readRowInput
	var wg sync.WaitGroup

	for _, tableID := range s.tables {

		wg.Add(1)
		offset := s.latestPositions.LatestPositions[tableID]

		rowInput = readRowInput{tableID: tableID, offset: offset, wg: &wg}

		go func(rowInput readRowInput) (err error) {
			// fmt.Println("The table ID inside go routine. Position: ", offset, tableID)
			sdk.Logger(s.ctx).Trace().Str("position", fmt.Sprintf("%d", offset)).Msg("The table ID inside go routine ")
			return s.ReadGoogleRow(rowInput, s.records)
		}(rowInput)

	}
	wg.Wait()
}
