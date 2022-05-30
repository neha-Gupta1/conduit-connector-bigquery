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
	"fmt"
	"regexp"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	googlebigquery "github.com/neha-Gupta1/conduit-connector-bigquery"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	cfg := map[string]string{
		googlebigquery.ConfigServiceAccount:     serviceAccount,
		googlebigquery.ConfigProjectID:          projectID,
		googlebigquery.ConfigDatasetID:          datasetID,
		googlebigquery.ConfigTableID:            "table_acceptance",
		googlebigquery.ConfigLocation:           location,
		googlebigquery.ConfigPrimaryKeyColName:  "name", //"table_acceptance"
		googlebigquery.ConfigIncrementalColName: "name",
		googlebigquery.ConfigPollingTime:        "1ms",
	}

	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		Config: AcceptanceSourceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: googlebigquery.Specification,
				// NewSource:        NewSource,
			},
			SourceConfig: cfg,
			GoleakOptions: []goleak.Option{
				goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"), //indirect leak from dependency go.opencensus.io
				goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			},
			AfterTest: func(t *testing.T) {
				err := cleanupDataset([]string{cfg[googlebigquery.ConfigTableID]})
				if err != nil {
					fmt.Println("Error found")
				}
			},
		},
	},
	)
}

// AcceptanceTestDriver implements sdk.AcceptanceTestDriver
type AcceptanceTestDriver struct {
	Config AcceptanceSourceTestDriverConfig
}

// AcceptanceSourceTestDriverConfig contains the configuration for
// AcceptanceTestDriver.
type AcceptanceSourceTestDriverConfig struct {
	// Connector is the connector to be tested.
	Connector sdk.Connector

	// SourceConfig config for source
	SourceConfig map[string]string

	// BeforeTest is executed before each acceptance test.
	BeforeTest func(t *testing.T)
	// AfterTest is executed after each acceptance test.
	AfterTest func(t *testing.T)

	// GoleakOptions will be applied to goleak.VerifyNone. Can be used to
	// suppress false positive goroutine leaks.
	GoleakOptions []goleak.Option

	// Skip is a slice of regular expressions used to identify tests that should
	// be skipped. The full test name will be matched against all regular
	// expressions and the test will be skipped if a match is found.
	Skip []string
}

func (d AcceptanceTestDriver) DestinationConfig(*testing.T) map[string]string {
	return map[string]string{}
}
func (d AcceptanceTestDriver) Connector() sdk.Connector {
	return d.Config.Connector
}

func (d AcceptanceTestDriver) SourceConfig(*testing.T) map[string]string {
	return d.Config.SourceConfig
}

func (d AcceptanceTestDriver) BeforeTest(t *testing.T) {
	// before test check if the test should be skipped
	d.Skip(t)

	if d.Config.BeforeTest != nil {
		d.Config.BeforeTest(t)
	}
}

func (d AcceptanceTestDriver) AfterTest(t *testing.T) {
	if d.Config.AfterTest != nil {
		d.Config.AfterTest(t)
	}
}

func (d AcceptanceTestDriver) Skip(t *testing.T) {
	var skipRegexs []*regexp.Regexp
	for _, skipRegex := range d.Config.Skip {
		r := regexp.MustCompile(skipRegex)
		skipRegexs = append(skipRegexs, r)
	}

	for _, skipRegex := range skipRegexs {
		if skipRegex.MatchString(t.Name()) {
			t.Skip(fmt.Sprintf("caller requested to skip tests that match the regex %q", skipRegex.String()))
		}
	}
}

func (d AcceptanceTestDriver) GoleakOptions(_ *testing.T) []goleak.Option {
	return d.Config.GoleakOptions
}

// WriteToSource writes data for source to pull data from
func (d AcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	var err error
	is := is.New(t)
	config := d.SourceConfig(t)
	records, err = writeToSource(config, records)
	is.NoErr(err)

	return records
}

func writeToSource(config map[string]string, records []sdk.Record) (result []sdk.Record, err error) {
	result, err = dataSetupWithRecord(config, records)
	return result, err
}
func (d AcceptanceTestDriver) ReadFromDestination(*testing.T, []sdk.Record) []sdk.Record {
	return []sdk.Record{}
}
