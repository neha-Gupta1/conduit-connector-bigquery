package googlesource

import (
	"context"
	"fmt"
	"strings"

	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"google.golang.org/api/option"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"gopkg.in/tomb.v2"
)

type Source struct {
	sdk.UnimplementedSource
	Session       *bqStoragepb.ReadSession
	BQReadClient  *bqStorage.BigQueryReadClient
	Config        googlebigquery.SourceConfig
	Tables        []string
	Ch            chan *bqStoragepb.AvroRows
	Ctx           context.Context
	ResponseCh    chan *[]string
	ResultCh      chan *[]string
	ErrResponseCh chan error
	SDKResponse   chan sdk.Record
	tomb          *tomb.Tomb
	readStream    string
	closeIterator chan bool
}

func NewSource() sdk.Source {
	return &Source{}
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring a Source Connector...")
	config, err := googlebigquery.ParseSourceConfig(cfg)
	if err != nil {
		sdk.Logger(ctx).Error().Msg("blank config provided")
		return err
	}

	s.Config = config
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) (err error) {

	s.Ctx = ctx
	bqReadClient, err := bqStorage.NewBigQueryReadClient(ctx, option.WithCredentialsFile(s.Config.Config.ConfigServiceAccount))
	if err != nil {
		sdk.Logger(s.Ctx).Info().Str("err", err.Error()).Msg("error found in NewBigQueryStorageClient client creation ")
		return err
	}

	s.BQReadClient = bqReadClient
	s.tomb = &tomb.Tomb{}

	if s.Config.Config.ConfigTableID == "" {
		s.Tables, err = s.listTables(s.Config.Config.ConfigProjectID, s.Config.Config.ConfigDatasetID)
		if err != nil {
			sdk.Logger(ctx).Info().Str("err", err.Error()).Msg("error found while listing table")
		}
	} else {
		s.Tables = strings.SplitAfter(s.Config.Config.ConfigTableID, ",")
	}

	s.SDKResponse = make(chan sdk.Record, 100)

	go s.Iterate(bqReadClient)
	// if err != nil {
	// 	sdk.Logger(ctx).Debug().Msg("end of function: open")
	// 	return err
	// }
	// for _, tableID := range s.Tables {
	// 	err = s.ReadDataFromEndpoint(bqReadClient, tableID)
	// }

	sdk.Logger(ctx).Debug().Msg("end of function: open")
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {

	sdk.Logger(ctx).Debug().Msg("Stated read function")
	var response sdk.Record
	if !s.HasNext() {
		sdk.Logger(ctx).Debug().Msg("no more values in repsonse. closing the channel now.")
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	response, err := s.Next(s.Ctx)
	if err != nil {
		sdk.Logger(ctx).Debug().Str("err", err.Error()).Msg("Error from endpoint.")
		return sdk.Record{}, err
	}
	return response, nil

}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	close(s.SDKResponse)
	s.BQReadClient.Close()
	s.CloseIterator()
	return nil
}

func (s *Source) CloseIterator() {
	fmt.Println("CLosing iterator....***********")
	s.closeIterator <- true
}
