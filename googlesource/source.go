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
	"errors"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"gopkg.in/tomb.v2"
)

type Source struct {
	sdk.UnimplementedSource
	// haris: let's check which fields do need to be exported.
	// It feels like most do not need to be.
	Session      *bqStoragepb.ReadSession
	BQReadClient *bigquery.Client
	SourceConfig googlebigquery.SourceConfig
	// haris: isn't this part of config?
	Tables []string
	// do we need Ctx? we have it in all the methods as a param
	Ctx         context.Context
	SDKResponse chan sdk.Record
	// haris: what's the difference between these two?
	LatestPositions latestPositions
	Position        Position
	ticker          *time.Ticker
	tomb            *tomb.Tomb
	iteratorClosed  chan bool
}

type latestPositions struct {
	// haris: what do the keys represent?
	// It feels like we're repeating info.
	// Firstly, we have a LatestPositions field in a latestPositions struct.
	// Also, IIUC, the keys here are actually table IDs, but we already have a table ID in the Position struct.
	LatestPositions map[string]Position
	lock            sync.Mutex
}

type Position struct {
	TableID string
	// can we use a page token instead?
	Offset int
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Trace().Msg("Configuring a Source Connector.")
	sourceConfig, err := googlebigquery.ParseSourceConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Str("err", err.Error()).Msg("invalid config provided")
		return err
	}

	s.SourceConfig = sourceConfig
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {
	s.Ctx = ctx
	s.Position = fetchPos(s, pos)

	// haris: can we then rename SDKResponse to just `records`? SDKResponse sounds a bit generic.
	// s.SDKResponse is a buffered channel that contains records
	//  coming from all the tables which user wants to sync.
	s.SDKResponse = make(chan sdk.Record, 100)
	s.iteratorClosed = make(chan bool, 2)
	s.ticker = time.NewTicker(googlebigquery.PollingTime)
	s.tomb = &tomb.Tomb{}

	// haris: why do we need a lock here?
	s.LatestPositions.lock.Lock()
	s.LatestPositions.LatestPositions = make(map[string]Position)
	s.LatestPositions.lock.Unlock()

	s.tomb.Go(s.runIterator)
	sdk.Logger(ctx).Trace().Msg("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Trace().Msg("Stated read function")
	var response sdk.Record

	response, err := s.Next(s.Ctx)
	if err != nil {
		sdk.Logger(ctx).Trace().Str("err", err.Error()).Msg("Error from endpoint.")
		return sdk.Record{}, err
	}
	return response, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	// TODO: understand why handling the closing of iterator fails the plugin

	// s.iteratorClosed <- true
	// sdk.Logger(s.Ctx).Error().Msg("Teardown: closing all channels")

	if s.SDKResponse != nil {
		close(s.SDKResponse)
	}
	err := s.StopIterator()
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("got error while closing bigquery client")
		return err
	}
	return nil
}

func (s *Source) StopIterator() error {
	if s.BQReadClient != nil {
		err := s.BQReadClient.Close()
		if err != nil {
			sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("got error while closing bigquery client")
			return err
		}
	}
	s.ticker.Stop()
	s.tomb.Kill(errors.New("iterator is stopped"))

	return nil
}
