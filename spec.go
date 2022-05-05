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

package googlebigquery

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "bigquery",
		Summary:     "A bigquery source plugin for Conduit, written in Go.",
		Description: "A plugin to fetch data from google bigquery",
		Version:     "v0.1.0",
		Author:      "Neha Gupta",
		SourceParams: map[string]sdk.Parameter{
			ConfigServiceAccount: { // TODO: can it be changed
				Default:     "",
				Required:    true,
				Description: "Path to service account key with data pulling access.", // We can also take it as value if required
			},
			ConfigProjectID: {
				Default:     "",
				Required:    true,
				Description: "Google project ID.",
			},
			ConfigDatasetID: {
				Default:     "",
				Required:    true,
				Description: "Google Bigqueries dataset ID.",
			},
			ConfigLocation: {
				Default:     "",
				Required:    true,
				Description: "Google Bigqueries dataset location.",
			},
			ConfigTableID: {
				Default:     "",
				Required:    false,
				Description: "Google Bigqueries table ID. Can provide `,` separated ID. Will pull whole dataset if no tableID provided.",
			},
		},
	}
}
