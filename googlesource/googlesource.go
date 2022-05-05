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
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	newClient = bigquery.NewClient
)

type readRowInput struct {
	tableID  string
	position Position
	wg       *sync.WaitGroup
}

// haris: why does rowInput need to be a chan?
func (s *Source) ReadGoogleRow(rowInput chan readRowInput, responseCh chan sdk.Record) (err error) {
	input := <-rowInput
	position := input.position
	tableID := input.tableID
	wg := input.wg

	lastRow := false
	offset := position.Offset

	defer wg.Done()
	for {
		// Keep on reading till end of table
		sdk.Logger(s.Ctx).Trace().Str("tableID", tableID).Msg("inside read google row infinite for loop")
		if lastRow {
			sdk.Logger(s.Ctx).Trace().Str("tableID", tableID).Msg("Its the last row. Done processing table")
			break
		}

		counter := 0
		// iterator
		it, err := s.runGetRow(offset, tableID)
		if err != nil {
			sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
			return err
		}

		for {
			var row []bigquery.Value
			// select statement to make sure channel was not closed by teardown stage
			select {
			case <-s.iteratorClosed:
				sdk.Logger(s.Ctx).Trace().Msg("recieved closed channel")
				return nil
			default:
				sdk.Logger(s.Ctx).Trace().Msg("iterator running")
			}

			err := it.Next(&row)
			Schema := it.Schema

			if err == iterator.Done {
				sdk.Logger(s.Ctx).Trace().Str("counter", fmt.Sprintf("%d", counter)).Msg("iterator is done.")
				if counter < googlebigquery.CounterLimit {
					// if counter is smaller than the limit we have reached the end of
					// iterator. And will break the for loop now.
					lastRow = true
				}
				break
			}
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("error while iterating")
				return err
			}

			// haris: does BQ have its own way of tracking rows, i.e. its own offsets?
			offset++
			position := Position{
				TableID: tableID,
				Offset:  offset,
			}

			counter++
			recPosition, err := json.Marshal(&position)
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error marshalling data")
				continue
			}

			// keep the track of last rows fetched for each table.
			// this helps in implementing incremental syncing.
			s.wrtieLatestPosition(position)

			data := make(sdk.StructuredData)
			for i, r := range row {
				data[Schema[i].Name] = r
			}

			record := sdk.Record{
				CreatedAt: time.Now().UTC(),
				Payload:   data,
				Position:  recPosition}

			responseCh <- record
		}
	}
	return
}

func (s *Source) wrtieLatestPosition(postion Position) {
	s.LatestPositions.lock.Lock()
	s.LatestPositions.LatestPositions[postion.TableID] = postion
	s.LatestPositions.lock.Unlock()
}

// runGetRow sync data for bigquery using bigquery client jobs
// haris proposal to rename to getRowIterator, since it's not returning a single row
func (s *Source) runGetRow(offset int, tableID string) (it *bigquery.RowIterator, err error) {
	// haris: does BigQuery guarantee ordering?
	q := s.BQReadClient.Query(
		"SELECT * FROM `" + s.SourceConfig.Config.ConfigProjectID + "." + s.SourceConfig.Config.ConfigDatasetID + "." + tableID + "` " +
			"LIMIT " + strconv.Itoa(googlebigquery.CounterLimit) + " OFFSET " + strconv.Itoa(offset))

	sdk.Logger(s.Ctx).Trace().Str("q ", q.Q)
	q.Location = s.SourceConfig.Config.ConfigLocation

	job, err := q.Run(s.tomb.Context(s.Ctx))
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	status, err := job.Wait(s.tomb.Context(s.Ctx))
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	if err := status.Err(); err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}

	it, err = job.Read(s.tomb.Context(s.Ctx))
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error while running job")
		return it, err
	}
	return it, err
}

// listTables demonstrates iterating through the collection of tables in a given dataset.
func (s *Source) listTables(projectID, datasetID string) ([]string, error) {
	ctx := context.Background()
	tables := []string{}

	client, err := newClient(ctx, projectID, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
	if err != nil {
		return []string{}, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	ts := client.Dataset(datasetID).Tables(ctx)
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
	case r := <-s.SDKResponse:
		return r, nil
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		return sdk.Record{}, sdk.ErrBackoffRetry
	}
}

func fetchPos(s *Source, pos sdk.Position) (position Position) {
	position = Position{TableID: "", Offset: 0}
	err := json.Unmarshal(pos, &position)
	if err != nil {
		sdk.Logger(s.Ctx).Info().Str("err", err.Error()).Msg("Could not get position. Will start with offset 0")
		position = Position{TableID: "", Offset: 0}
	}
	return position
}

func getTables(s *Source) (err error) {
	if s.SourceConfig.Config.ConfigTableID == "" {
		s.Tables, err = s.listTables(s.SourceConfig.Config.ConfigProjectID, s.SourceConfig.Config.ConfigDatasetID)
		if err != nil {
			sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found while listing table")
		}
	} else {
		s.Tables = strings.SplitAfter(s.SourceConfig.Config.ConfigTableID, ",")
	}
	return err
}

// split into more methods for readability
func (s *Source) runIterator() (err error) {
	rowInput := make(chan readRowInput)
	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()
		case <-s.ticker.C:
			sdk.Logger(s.Ctx).Trace().Msg("ticker started ")
			// create new client everytime the new sync start. This make sure that new tables coming in are handled.
			// haris: can we list tables in a way which doesn't require us to create a new client every polling period?
			// in other words, why can't we list all the tables with an existing client?
			// I'm concerned about the time overhead but also about new connections.
			client, err := newClient(s.tomb.Context(s.Ctx), s.SourceConfig.Config.ConfigProjectID, option.WithCredentialsFile(s.SourceConfig.Config.ConfigServiceAccount))
			if err != nil {
				sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("error found while creating connection. ")
				s.tomb.Kill(err)
				return fmt.Errorf("bigquery.NewClient: %v", err)
			}

			s.BQReadClient = client

			if err = getTables(s); err != nil {
				sdk.Logger(s.Ctx).Trace().Str("err", err.Error()).Msg("error found while fetching tables. Need to stop proccessing ")
				return err
			}

			foundTable := false

			// if its an already running pipeline and we just
			// want to check for any new rows. Send the offset as
			// last position where we left.
			if len(s.LatestPositions.LatestPositions) > 0 {
				// wait group make sure that we start new iteration only
				//  after the first iteration is completely done.
				var wg sync.WaitGroup
				for _, tableID := range s.Tables {
					wg.Add(1)
					position := s.LatestPositions.LatestPositions[tableID]

					s.tomb.Go(func() (err error) {
						sdk.Logger(s.Ctx).Trace().Str("position", position.TableID).Msg("The table ID inside go routine ")
						return s.ReadGoogleRow(rowInput, s.SDKResponse)
					})
					rowInput <- readRowInput{tableID: tableID, position: position, wg: &wg}
				}
				wg.Wait()
			} else {
				// if the pipeline has been newly started and it was earlier synced.
				// Then we want to skip all the tables which are already synced and
				// pull only after specified position

				var wg sync.WaitGroup

				for _, tableID := range s.Tables {
					wg.Add(1)
					if !foundTable && s.Position.TableID != "" {
						if s.Position.TableID != tableID {
							continue
						} else {
							foundTable = true
						}
					}

					s.tomb.Go(func() (err error) {
						sdk.Logger(s.Ctx).Trace().Str("position", s.Position.TableID).Msg("The table ID inside go routine ")
						return s.ReadGoogleRow(rowInput, s.SDKResponse)
					})
					rowInput <- readRowInput{tableID: tableID, position: s.Position, wg: &wg}
				}
				wg.Wait()
			}
		}
	}
}
