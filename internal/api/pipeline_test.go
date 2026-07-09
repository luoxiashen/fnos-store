package api

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fnos-store/internal/platform"
)

// stubAppCenter is a scripted platform.AppCenter for verifyInstalled tests.
// Only Check and List participate in the assertions; the other methods are
// no-ops satisfying the interface.
type stubAppCenter struct {
	checkScript []stubCheckResult
	listResult  []platform.InstalledApp
	listErr     error

	nCheck int32
	nList  int32
}

type stubCheckResult struct {
	installed bool
	err       error
}

func (s *stubAppCenter) Check(appname string) (bool, error) {
	idx := int(atomic.AddInt32(&s.nCheck, 1)) - 1
	if idx >= len(s.checkScript) {
		// Script exhausted: repeat the last entry so long loops don't panic.
		idx = len(s.checkScript) - 1
	}
	r := s.checkScript[idx]
	return r.installed, r.err
}

func (s *stubAppCenter) List() ([]platform.InstalledApp, error) {
	atomic.AddInt32(&s.nList, 1)
	return s.listResult, s.listErr
}

func (s *stubAppCenter) Status(string) (string, error)                 { return "", nil }
func (s *stubAppCenter) InstallFpk(string, int) error                  { return nil }
func (s *stubAppCenter) InstallLocal(string, int, bool) error          { return nil }
func (s *stubAppCenter) Uninstall(string) error                        { return nil }
func (s *stubAppCenter) Start(string) error                            { return nil }
func (s *stubAppCenter) Stop(string) error                             { return nil }
func (s *stubAppCenter) DefaultVolume() (int, error)                   { return 1, nil }
func (s *stubAppCenter) ListVolumes() ([]platform.VolumeInfo, error)   { return nil, nil }

// TestVerifyInstalled locks in the retry + List() fallback contract for
// GitHub issue conversun/fnos-apps#181. Cases cover the happy path, the
// race-recovery path that motivates the fix, the List() fallback (accept
// only running/stopped, reject unknown), a hard CLI error that MUST NOT
// retry, and ctx cancellation.
func TestVerifyInstalled(t *testing.T) {
	// Skip real sleeps but keep ctx-cancellation semantics.
	origWait := verifyWait
	verifyWait = func(ctx context.Context, _ time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t.Cleanup(func() { verifyWait = origWait })

	cases := []struct {
		name        string
		checkScript []stubCheckResult
		list        []platform.InstalledApp
		listErr     error
		preCancel   bool
		wantErr     bool
		wantErrSub  string
		wantCtxErr  bool
		wantNCheck  int32
		wantNList   int32
	}{
		{
			name:        "happy_first_try",
			checkScript: []stubCheckResult{{installed: true}},
			wantNCheck:  1,
			wantNList:   0,
		},
		{
			name: "race_recovers_at_4",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false},
				{installed: true},
			},
			wantNCheck: 4,
			wantNList:  0,
		},
		{
			name: "list_fallback_hit_running",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
			},
			list:       []platform.InstalledApp{{AppName: "plexmediaserver", Status: "running"}},
			wantNCheck: 8,
			wantNList:  1,
		},
		{
			name: "list_fallback_hit_stopped",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
			},
			list:       []platform.InstalledApp{{AppName: "plexmediaserver", Status: "stopped"}},
			wantNCheck: 8,
			wantNList:  1,
		},
		{
			name: "list_fallback_reject_unknown_status",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
			},
			list:       []platform.InstalledApp{{AppName: "plexmediaserver", Status: "unknown"}},
			wantErr:    true,
			wantErrSub: "验证失败",
			wantNCheck: 8,
			wantNList:  1,
		},
		{
			name: "list_fallback_wrong_appname",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
			},
			list:       []platform.InstalledApp{{AppName: "other-app", Status: "running"}},
			wantErr:    true,
			wantErrSub: "验证失败",
			wantNCheck: 8,
			wantNList:  1,
		},
		{
			name: "list_fallback_miss_empty",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
				{installed: false}, {installed: false}, {installed: false}, {installed: false},
			},
			list:       nil,
			wantErr:    true,
			wantErrSub: "验证失败",
			wantNCheck: 8,
			wantNList:  1,
		},
		{
			name:        "hard_error_no_retry",
			checkScript: []stubCheckResult{{installed: false, err: errors.New("cli exit 2")}},
			wantErr:     true,
			wantErrSub:  "cli exit 2",
			wantNCheck:  1,
			wantNList:   0,
		},
		{
			name: "ctx_already_canceled",
			checkScript: []stubCheckResult{
				{installed: false}, {installed: false},
			},
			preCancel:  true,
			wantErr:    true,
			wantCtxErr: true,
			wantNCheck: 0,
			wantNList:  0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubAppCenter{
				checkScript: tc.checkScript,
				listResult:  tc.list,
				listErr:     tc.listErr,
			}
			p := &installPipeline{
				queue: NewOperationQueue(),
				ac:    stub,
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.preCancel {
				cancel()
			} else {
				t.Cleanup(cancel)
			}

			err := p.verifyInstalled(ctx, "plexmediaserver")

			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErrSub != "" && err != nil {
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantErrSub)
				}
			}
			if tc.wantCtxErr && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("expected ctx error, got %v", err)
			}
			if got := atomic.LoadInt32(&stub.nCheck); got != tc.wantNCheck {
				t.Errorf("nCheck = %d, want %d", got, tc.wantNCheck)
			}
			if got := atomic.LoadInt32(&stub.nList); got != tc.wantNList {
				t.Errorf("nList = %d, want %d", got, tc.wantNList)
			}
		})
	}
}
