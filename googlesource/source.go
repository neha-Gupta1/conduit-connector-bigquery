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
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
	"gopkg.in/tomb.v2"
)

type Source struct {
	sdk.UnimplementedSource
	bqReadClient *bigquery.Client
	sourceConfig googlebigquery.SourceConfig
	// table to be synced
	table string
	// do we need Ctx? we have it in all the methods as a param
	// Neha: for all the function running in goroutine we needed the ctx value. To provide the current
	// ctx value ctx was required in struct.
	ctx            context.Context
	records        chan sdk.Record
	position       string
	ticker         *time.Ticker
	tomb           *tomb.Tomb
	iteratorClosed chan bool
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

	s.sourceConfig = sourceConfig
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {
	s.ctx = ctx
	fetchPos(s, pos)

	pollingTime := googlebigquery.PollingTime

	// s.records is a buffered channel that contains records
	//  coming from all the tables which user wants to sync.
	s.records = make(chan sdk.Record, 100)
	s.iteratorClosed = make(chan bool, 2)

	if len(s.sourceConfig.Config.PollingTime) > 0 {
		pollingTime, err = time.ParseDuration(s.sourceConfig.Config.PollingTime)
		if err != nil {
			sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("error found while getting time.")
			return errors.New("invalid polling time duration provided")
		}
	}

	s.ticker = time.NewTicker(pollingTime)
	s.tomb = &tomb.Tomb{}

	client, err := newClient(s.tomb.Context(s.ctx), s.sourceConfig.Config.ProjectID, option.WithCredentialsFile(s.sourceConfig.Config.ServiceAccount))
	if err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("error found while creating connection. ")
		clientErr := fmt.Errorf("error while creating bigquery client: %s", err.Error())
		s.tomb.Kill(clientErr)
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}

	s.bqReadClient = client

	s.tomb.Go(s.runIterator)
	sdk.Logger(ctx).Trace().Msg("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Trace().Msg("Stated read function")
	var response sdk.Record

	response, err := s.Next(s.ctx)
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
	if s.records != nil {
		close(s.records)
	}
	err := s.StopIterator()
	if err != nil {
		sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("got error while closing BigQuery client")
		return err
	}
	return nil
}

func (s *Source) StopIterator() error {
	if s.bqReadClient != nil {
		err := s.bqReadClient.Close()
		if err != nil {
			sdk.Logger(s.ctx).Error().Str("err", err.Error()).Msg("got error while closing BigQuery client")
			return err
		}
	}
	if s.ticker != nil {
		s.ticker.Stop()
	}
	if s.tomb != nil {
		s.tomb.Kill(errors.New("iterator is stopped"))
	}

	return nil
}
