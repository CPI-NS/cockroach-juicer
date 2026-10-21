// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"
)

type snapshot struct {
	URL   string `json:"url"`
	Start int64  `json:"start_unix_ns"`
	End   int64  `json:"end_unix_ns"`
	Raw   string `json:"raw"`
	Error string `json:"error,omitempty"`
}

func observe(
	ctx context.Context, urls []string, start, end time.Time, path string,
) ([]snapshot, error) {
	var rows []snapshot
	if len(urls) == 0 {
		return rows, nil
	}
	f, e := os.Create(path)
	if e != nil {
		return rows, e
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	boundaries := map[string][]snapshot{}
	if !sleep(ctx, time.Until(start)) {
		return rows, nil
	}
	client := &http.Client{Timeout: time.Second}
	for ctx.Err() == nil && time.Now().Before(end) {
		for _, url := range urls {
			row := snapshot{URL: url, Start: time.Now().UnixNano()}
			req, e := http.NewRequestWithContext(ctx, "GET", url, nil)
			if e == nil {
				var resp *http.Response
				resp, e = client.Do(req)
				if e == nil {
					var raw []byte
					raw, e = io.ReadAll(io.LimitReader(resp.Body, 32<<20))
					resp.Body.Close()
					row.Raw = string(raw)
					if resp.StatusCode != 200 {
						row.Error = resp.Status
					}
				}
			}
			if e != nil {
				row.Error = e.Error()
			}
			row.End = time.Now().UnixNano()
			if e := encoder.Encode(row); e != nil {
				return nil, e
			}
			if row.Error == "" && row.Start >= start.UnixNano() && row.End < end.UnixNano() {
				pair := boundaries[url]
				if len(pair) < 2 {
					pair = append(pair, row)
				} else {
					pair[1] = row
				}
				boundaries[url] = pair
			}
		}
		if !sleep(ctx, time.Second) {
			break
		}
	}
	for _, url := range urls {
		rows = append(rows, boundaries[url]...)
	}
	return rows, f.Sync()
}
