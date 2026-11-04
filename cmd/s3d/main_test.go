package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestAdminBasicAuth(t *testing.T) {
	const password = "secret"
	handler := adminBasicAuth(password)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name          string
		username      string
		password      string
		setAuth       bool
		wantStatus    int
		wantChallenge bool
	}{
		{name: "missing credentials", wantStatus: http.StatusUnauthorized, wantChallenge: true},
		{name: "wrong password", username: "admin", password: "wrong", setAuth: true, wantStatus: http.StatusUnauthorized, wantChallenge: true},
		{name: "correct password with username", username: "admin", password: password, setAuth: true, wantStatus: http.StatusNoContent},
		{name: "correct password without username", password: password, setAuth: true, wantStatus: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/repack", nil)
			if tc.setAuth {
				req.SetBasicAuth(tc.username, tc.password)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, rec.Code)
			}
			challenge := rec.Header().Get("WWW-Authenticate")
			if tc.wantChallenge {
				if challenge != adminAuthChallenge {
					t.Fatalf("expected challenge %q, got %q", adminAuthChallenge, challenge)
				}
			} else if challenge != "" {
				t.Fatalf("unexpected challenge %q", challenge)
			}
		})
	}
}

func TestValidateServerConfig(t *testing.T) {
	valid := Config{
		ApiAddress:    "127.0.0.1:8000",
		AdminAddress:  "127.0.0.1:8001",
		AdminPassword: "password",
		Sia: Sia{
			DataShards:   10,
			ParityShards: 20,
		},
	}

	if err := validateServerConfig(valid); err != nil {
		t.Fatal(err)
	}

	// reject redundancy the SDK would fail on later
	invalid := []struct {
		dataShards   uint8
		parityShards uint8
	}{
		{0, 20},
		{10, 0},
		{10, 4},
		{10, 60},
		{200, 200},
	}

	for _, tc := range invalid {
		c := valid
		c.Sia.DataShards = tc.dataShards
		c.Sia.ParityShards = tc.parityShards
		if err := validateServerConfig(c); err == nil {
			t.Fatal("expected error", tc.dataShards, tc.parityShards)
		}
	}
}

func TestTryLoadConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not restrict reads on windows")
	} else if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}

	orig := cfg
	t.Cleanup(func() { cfg = orig })

	unreadable := t.TempDir()
	if err := os.WriteFile(filepath.Join(unreadable, "s3d.yml"), []byte("directory: /unreadable\n"), 0o000); err != nil {
		t.Fatal(err)
	}
	t.Chdir(unreadable)

	dataDir := t.TempDir()
	readable := filepath.Join(dataDir, "s3d.yml")
	if err := os.WriteFile(readable, []byte("directory: /marker\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv(configFileEnvVar, "")
	t.Setenv(dataDirEnvVar, dataDir)

	if fp := tryLoadConfig(); fp != readable {
		t.Fatal("unexpected path", fp)
	} else if cfg.Directory != "/marker" {
		t.Fatal("unexpected directory", cfg.Directory)
	}
}
