package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFileRedundancy(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "s3d.yml")
	if err := os.WriteFile(fp, []byte(`sia:
  dataShards: 24
  parityShards: 24
`), 0600); err != nil {
		t.Fatal(err)
	}

	var cfg Config
	if err := LoadFile(fp, &cfg); err != nil {
		t.Fatal(err)
	} else if cfg.Sia.DataShards != 24 || cfg.Sia.ParityShards != 24 {
		t.Fatalf("unexpected redundancy: %d+%d", cfg.Sia.DataShards, cfg.Sia.ParityShards)
	}
}

