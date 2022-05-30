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
	tableID   string
	offset    string
	positions string
	wg        *sync.WaitGroup
}

// checkInitialPos helps in creating the query to fetch data from endpoint
func (s *Source) checkInitialPos(positions string, incrementColName string, tableID string, primaryColName string) (firstSync, userDefinedOffset bool, userDefinedKey bool) {
	// if its the firstSync no offset is applied
	if positions == "" {
		firstSync = true
	}

	// if incrementColName set - we orderBy the provided column name
	if len(incrementColName) > 0 {
		userDefinedOffset = true
	}

	// if primaryColName set - we orderBy the provided column name
	if len(primaryColName) > 0 {
		userDefinedKey = true
	}

	return firstSync, userDefinedOffset, userDefinedKey
}

func (s *Source) ReadGoogleRow(rowInput readRowInput, responseCh chan sdk.Record) (err error) {
	sdk.Logger(s.ctx).Trace().Msg("Inside read google row")
	var userDefinedOffset, userDefinedKey bool
	var firstSync bool

	offset := rowInput.offset
	tableID := s.table
	wg := rowInput.wg

	firstSync, userDefinedOffset, userDefinedKey = s.checkInitialPos(rowInput.positions, s.sourceConfig.Config.IncrementColNames, tableID, s.sourceConfig.Config.PrimaryKeyColNames)
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
		it, err := s.getRowIterator(offset, tableID, firstSync)
		if err != nil && strings.Contains(err.Error(), "Not found") {
			sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return nil
		}
		if err != nil {
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
			schema := it.Schema

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

			data := make(sdk.StructuredData)
			var key string

			for i, r := range row {
				// handle dates
				if schema[i].Type == bigquery.TimestampFieldType {
					dateR := fmt.Sprintf("%v", r)
					dateLocal, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", dateR)
					if err != nil {
						sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error while converting to time format")
						return err
					}
					r = dateLocal.Format("2006-01-02 15:04:05.999999 MST")
				}
				data[schema[i].Name] = r

				// if we have found the user provided incremental key that would be used as offset
				if userDefinedOffset {
					if schema[i].Name == s.sourceConfig.Config.IncrementColNames {
						offset = fmt.Sprint(data[schema[i].Name])
						offset = getType(schema[i].Type, offset)
					}
				} else {
					offset, err = calcOffset(firstSync, offset)
					if err != nil {
						sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error marshalling key")
						continue
					}
				}

				// if we have found the user provided incremental key that would be used as offset
				if userDefinedKey {
					if schema[i].Name == s.sourceConfig.Config.PrimaryKeyColNames {
						key = fmt.Sprintf("%v", data[schema[i].Name])
					}
				}
			}

			buffer := &bytes.Buffer{}
			if err := gob.NewEncoder(buffer).Encode(key); err != nil {
				sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error marshalling key")
				continue
			}
			byteKey := buffer.Bytes()

			counter++
			firstSync = false

			// keep the track of last rows fetched for each table.
			// this helps in implementing incremental syncing.
			recPosition, err := s.writePosition(offset)
			if err != nil {
				sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			record := sdk.Record{
				CreatedAt: time.Now().UTC(),
				Payload:   data,
				Key:       sdk.RawData(byteKey),
				Position:  recPosition}

			responseCh <- record
		}
	}
	return
}

func calcOffset(firstSync bool, offset string) (string, error) {
	// if user doesn't provide any incremental key we manually create offsets to pull data
	if firstSync {
		offset = "0"
	}
	offsetInt, err := strconv.Atoi(offset)
	if err != nil {
		return offset, err
	}
	offsetInt++
	offset = fmt.Sprintf("%d", offsetInt)
	return offset, err
}

func getType(fieldType bigquery.FieldType, offset string) string {
	switch fieldType {
	case bigquery.IntegerFieldType:
		return offset
	case bigquery.FloatFieldType:
		return offset
	case bigquery.NumericFieldType:
		return offset
	case bigquery.TimeFieldType:
		return fmt.Sprintf("'%s'", offset)

	default:
		return fmt.Sprintf("'%s'", offset)
	}
}

// writePosition prevents race condition happening while using map inside goroutine
func (s *Source) writePosition(offset string) (recPosition []byte, err error) {
	s.position = offset
	return json.Marshal(&s.position)
}

// getRowIterator sync data for bigquery using bigquery client jobs
func (s *Source) getRowIterator(offset string, tableID string, firstSync bool) (it *bigquery.RowIterator, err error) {
	// check for config `IncrementColNames`. User can provide the column name which
	// would be used as orderBy as well as incremental or offset value. Orderby is not mandatory though

	var query string
	if len(s.sourceConfig.Config.IncrementColNames) > 0 {
		columnName := s.sourceConfig.Config.IncrementColNames
		if firstSync {
			query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
				" ORDER BY " + columnName + " LIMIT " + strconv.Itoa(googlebigquery.CounterLimit)
		} else {
			query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` WHERE " + columnName +
				" > " + offset + " ORDER BY " + columnName + " LIMIT " + strconv.Itoa(googlebigquery.CounterLimit)
		}
	} else {
		// add default value if none specified
		if len(offset) == 0 {
			offset = "0"
		}
		// if no incremental value provided using default offset which is created by incrementing a counter each time a row is sync.
		query = "SELECT * FROM `" + s.sourceConfig.Config.ProjectID + "." + s.sourceConfig.Config.DatasetID + "." + tableID + "` " +
			" LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + offset
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
	s.position = ""

	err := json.Unmarshal(pos, &s.position)
	if err != nil {
		sdk.Logger(s.ctx).Info().Msg("Could not get position. Will start with offset 0")
	}
}

func getTables(s *Source) (err error) {
	if s.sourceConfig.Config.TableIDs == "" {
		sdk.Logger(s.ctx).Trace().Str("err", err.Error()).Msg("error found while listing table")
		return fmt.Errorf("table ID blank")
	}
	s.table = s.sourceConfig.Config.TableIDs
	return err
}

func (s *Source) runIterator() (err error) {
	var wg sync.WaitGroup

	if err = getTables(s); err != nil {
		sdk.Logger(s.ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
		return err
	}

	// Snapshot sync. Start were we left last
	wg.Add(1)

	rowInput := readRowInput{offset: s.position, positions: s.position, wg: &wg}
	s.tomb.Go(func() (err error) {
		sdk.Logger(s.ctx).Trace().Msg(fmt.Sprintf("position %v : %v", s.table, s.position))
		return s.ReadGoogleRow(rowInput, s.records)
	})

	wg.Wait()

	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()
		case <-s.ticker.C:
			sdk.Logger(s.ctx).Trace().Msg("ticker started ")
			runCDCIterator(s, rowInput)
		}
	}
}

func runCDCIterator(s *Source, rowInput readRowInput) {
	// wait group make sure that we start new iteration only
	//  after the first iteration is completely done.
	var wg sync.WaitGroup
	wg.Add(1)
	rowInput = readRowInput{tableID: s.table, offset: s.position, positions: s.position, wg: &wg}

	s.tomb.Go(func() (err error) {
		sdk.Logger(s.ctx).Trace().Msg(fmt.Sprintf("position %v : %v", s.table, s.position))
		return s.ReadGoogleRow(rowInput, s.records)
	})

	wg.Wait()
}
