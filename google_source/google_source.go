package googlesource

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	bqStorage "cloud.google.com/go/bigquery/storage/apiv1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	gax "github.com/googleapis/gax-go/v2"
	goavro "github.com/linkedin/goavro/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	bqStoragepb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
	"google.golang.org/grpc"
)

// rpcOpts is used to configure the underlying gRPC client to accept large
// messages.  The BigQuery Storage API may send message blocks up to 128MB
// in size.
var rpcOpts = gax.WithGRPCOptions(
	grpc.MaxCallRecvMsgSize(1024 * 1024 * 129),
)

// Command-line flags.
var (
	snapshotMillis = flag.Int64("snapshot_millis", 0,
		"Snapshot time to use for reads, represented in epoch milliseconds format.  Default behavior reads current data.")
)

// ReadDataFromEndpoint read data from google bigquery
func (s *Source) ReadDataFromEndpoint(bqReadClient *bqStorage.BigQueryReadClient, tableID string) (err error) {

	readTable := fmt.Sprintf("projects/%s/datasets/%s/tables/%s",
		s.Config.Config.ConfigProjectID,
		s.Config.Config.ConfigDatasetID,
		tableID,
	)

	tableReadOptions := &bqStoragepb.ReadSession_TableReadOptions{}

	createReadSessionRequest := &bqStoragepb.CreateReadSessionRequest{
		Parent: fmt.Sprintf("projects/%s", s.Config.Config.ConfigProjectID),
		ReadSession: &bqStoragepb.ReadSession{
			Table:       readTable,
			DataFormat:  bqStoragepb.DataFormat_AVRO,
			ReadOptions: tableReadOptions,
		},
		MaxStreamCount: 1,
	}

	//TODO: discuss if this is required

	// // Set a snapshot time if it's been specified.
	// if *snapshotMillis > 0 {
	// 	ts, err := ptypes.TimestampProto(time.Unix(0, *snapshotMillis*1000))
	// 	if err != nil {
	// 		sdk.Logger(s.Ctx).Info().Str("snapsortmilis", fmt.Sprint(*snapshotMillis)).Msg("Invalid snapshot millis (%d): %v")
	// 		return err
	// 	}
	// 	createReadSessionRequest.ReadSession.TableModifiers = &bqStoragepb.ReadSession_TableModifiers{
	// 		SnapshotTime: ts,
	// 	}
	// }

	// Create the session from the request.
	s.Session, err = bqReadClient.CreateReadSession(s.Ctx, createReadSessionRequest, rpcOpts)
	if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error found while creating session: ")
		return err
	}

	if len(s.Session.GetStreams()) == 0 {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("no streams in session.  if this was a small query result, consider writing to output to a named table.")
		return errors.New("no session found")
	}

	s.readStream = s.Session.GetStreams()[0].Name
	s.Ch = make(chan *bqStoragepb.AvroRows, 1)

	// s.tomb.Go(s.PullData)
	// s.tomb.Go(s.FlushingData)

	go s.PullData()
	go s.FlushingData()

	return err

}

func (s *Source) PullData() (err error) {
	defer close(s.Ch)
	if err := processStream(s.Ctx, s.BQReadClient, s.readStream, s.Ch); err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("processStream failure")
	}
	return err
}

func (s *Source) FlushingData() (err error) {
	err = processAvro(s.Ctx, s.Session.GetAvroSchema().GetSchema(), s.Ch, s.SDKResponse)
	if err != nil && err == sdk.ErrBackoffRetry {
		sdk.Logger(s.Ctx).Info().Msg("Done processing table")
		return
	} else if err != nil {
		sdk.Logger(s.Ctx).Error().Str("err", err.Error()).Msg("Error found")
		return
	}
	return err
}

// processStream reads rows from a single storage Stream, and sends the Avro
// data blocks to a channel. This function will retry on transient stream
// failures and bookmark progress to avoid re-reading data that's already been
// successfully transmitted.
func processStream(ctx context.Context, client *bqStorage.BigQueryReadClient, st string, ch chan<- *bqStoragepb.AvroRows) error {
	var offset int64

	// Streams may be long-running.  Rather than using a global retry for the
	// stream, implement a retry that resets once progress is made.
	retryLimit := 3

	for {
		retries := 0

		rowStream, err := client.ReadRows(ctx, &bqStoragepb.ReadRowsRequest{
			ReadStream: st,
			Offset:     offset,
		}, rpcOpts)
		if err != nil {
			return fmt.Errorf("couldn't invoke ReadRows: %v", err)
		}

		// Process the streamed responses.
		for {
			r, err := rowStream.Recv()
			if err == io.EOF {
				return nil
			}

			fmt.Println("Printing r: ", r)
			if err != nil {
				retries++
				if retries >= retryLimit {
					return fmt.Errorf("processStream retries exhausted: %v", err)
				}
				break
			}

			rc := r.GetRowCount()
			if rc > 0 {
				offset = offset + rc
				retries = 0
				ch <- r.GetAvroRows()

			}
		}
	}
}

// processAvro receives row blocks from a channel, and uses the provided Avro
// schema to decode the blocks into individual row messages for printing.  Will
// continue to run until the channel is closed or the provided context is
// cancelled.
func processAvro(ctx context.Context, schema string, ch <-chan *bqStoragepb.AvroRows, responseCh chan<- sdk.Record) error {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return fmt.Errorf("couldn't create codec: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case rows, ok := <-ch:
			if !ok {
				return sdk.ErrBackoffRetry
			}
			undecoded := rows.GetSerializedBinaryRows()
			for len(undecoded) > 0 {
				datum, remainingBytes, err := codec.NativeFromBinary(undecoded)

				if err != nil {
					if err == io.EOF {
						err = nil
						break
					}
					return fmt.Errorf("decoding error with %d bytes remaining: %v", len(undecoded), err)
				}
				response := printDatum(datum)
				// Metadata: map[string]string{
				// 	"action": "delete",
				// },
				// Position:  p.ToRecordPosition(),
				// Key:       sdk.RawData(entry.key),
				// CreatedAt: entry.lastModified,

				buffer := &bytes.Buffer{}
				gob.NewEncoder(buffer).Encode(response)
				byteSlice := buffer.Bytes()

				record := sdk.Record{
					CreatedAt: time.Now().UTC(),
					Payload:   sdk.RawData(byteSlice)}

				responseCh <- record
				undecoded = remainingBytes
			}
		}
	}
}

// listTables demonstrates iterating through the collection of tables in a given dataset.
func (s *Source) listTables(projectID, datasetID string) ([]string, error) {
	ctx := context.Background()
	tables := []string{}

	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(s.Config.Config.ConfigServiceAccount))
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

// printDatum prints the decoded row datum.
func printDatum(d interface{}) (response []string) {
	log.Println("Came here")
	m, ok := d.(map[string]interface{})
	if !ok {
		log.Printf("failed type assertion: %v", d)
	}
	// Go's map implementation returns keys in a random ordering, so we sort the keys before accessing.
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		response = append(response, fmt.Sprintf("%s: %-20v \n", key, valueFromTypeMap(m[key])))
	}

	return response
}

// valueFromTypeMap returns the first value/key in the type map.  This function
// is only suitable for simple schemas, as complex typing such as arrays and
// records necessitate a more robust implementation.  See the goavro library
// and the Avro specification for more information.
func valueFromTypeMap(field interface{}) interface{} {
	m, ok := field.(map[string]interface{})
	if !ok {
		return nil
	}
	for _, v := range m {
		return v
	}
	return nil
}

// Next returns the next record from the buffer.
func (s *Source) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case r := <-s.SDKResponse:
		return r, nil
	case <-s.tomb.Dead():
		return sdk.Record{}, s.tomb.Err()
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

func (s *Source) HasNext() bool {
	if len(s.SDKResponse) <= 0 {
		sdk.Logger(s.Ctx).Debug().Msg("We will try in 2 seconds.")
		time.Sleep(2 * time.Second)
		if len(s.SDKResponse) <= 0 {
			sdk.Logger(s.Ctx).Debug().Msg("We are done with pulling info")
			return false
		}
	}
	return true
}

func (s *Source) Iterate(bqReadClient *bqStorage.BigQueryReadClient) (err error) {

	for {
		select {
		case <-s.closeIterator:
			log.Println("Got context.done")
			return nil
		default:
			log.Println("default called")
			for _, tableID := range s.Tables {
				err = s.ReadDataFromEndpoint(bqReadClient, tableID)
				if err != nil {
					fmt.Println("Error found")
					return

				}
			}
		}
	}
}
