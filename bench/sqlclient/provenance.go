// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func digest(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func verify(c config) error {
	if len(c.ArtifactPaths) != 2 || c.SourceRevision == "" {
		return errors.New("artifact paths and source revision required")
	}
	hashes := map[string]string{}
	for name, path := range c.ArtifactPaths {
		stat, e := os.Stat(path)
		if e != nil {
			return e
		}
		if !stat.Mode().IsRegular() || stat.Mode()&0111 == 0 {
			return fmt.Errorf("artifact is not an executable file: %s", path)
		}
		hash, e := digest(path)
		if e != nil {
			return e
		}
		hashes[name] = hash
		if expected := c.ArtifactHashes[name]; expected != "" && expected != hash {
			return fmt.Errorf("artifact SHA-256 mismatch: %s", name)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	version, e := exec.CommandContext(ctx, c.ArtifactPaths["cockroach"], "version").CombinedOutput()
	if e != nil {
		return fmt.Errorf("cockroach version probe: %w", e)
	}
	out := map[string]any{"schema_version": 1, "source_revision": c.SourceRevision, "sha256": hashes, "paths": c.ArtifactPaths, "version": string(version), "os": runtime.GOOS, "arch": runtime.GOARCH}
	if c.SourceManifest != "" {
		hash, e := digest(c.SourceManifest)
		if e != nil {
			return e
		}
		raw, e := os.ReadFile(c.SourceManifest)
		if e != nil {
			return e
		}
		out["source_manifest_sha256"] = hash
		out["source_manifest_raw"] = string(raw)
	}
	return writeJSON(c.Output+".provenance.json", out)
}
func evidence(c config) error {
	entries, e := os.ReadDir(c.LogDir)
	if e != nil {
		return e
	}
	var files []map[string]any
	var banners []string
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		path := filepath.Join(c.LogDir, entry.Name())
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		raw, e := io.ReadAll(io.LimitReader(f, 64<<20))
		f.Close()
		if e != nil {
			return e
		}
		hash, e := digest(path)
		if e != nil {
			return e
		}
		files = append(files, map[string]any{"path": entry.Name(), "sha256": hash, "bytes_scanned": len(raw)})
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.Contains(strings.ToLower(line), "juicer") {
				banners = append(banners, line)
			}
		}
	}
	enabled := false
	for _, line := range banners {
		if strings.Contains(line, "juicer: interception enabled") {
			enabled = true
		}
	}
	calibrationObserved := false
	for _, line := range banners {
		if strings.Contains(line, "HOLD CALIBRATION mode:") {
			calibrationObserved = true
		}
	}
	out := map[string]any{"hold_calibration": c.HoldCalibration, "calibration_observed": calibrationObserved, "schema_version": 1, "expected_enabled": c.JuicerEnabled, "interception_observed": enabled, "valid": enabled == c.JuicerEnabled, "juicer_log_lines": banners, "logs": files}
	if e := writeJSON(c.Output+".evidence.json", out); e != nil {
		return e
	}
	if c.HoldCalibration && !calibrationObserved {
		return errors.New("observe-only hold calibration declaration missing")
	}
	if enabled != c.JuicerEnabled {
		return errors.New("Juicer interception evidence disagrees with requested configuration")
	}
	return nil
}
