package fshttp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/require"
)

// TestTimeoutDoesNotBoundSlowTrickle demonstrates that --timeout (ci.Timeout) is a
// pure per-Read/Write idle deadline (see timeoutConn.nudgeDeadline in dialer.go),
// not a bound on total request duration or on minimum throughput. A peer that
// drips even a single byte more often than the configured timeout can hold a
// connection open indefinitely -- and rclone's own --retries / --low-level-retries
// never engage, because no Read/Write ever actually errors.
//
// This reproduces the mechanism suspected behind a ~16h hung `rclone copy`
// multipart upload to a DigitalOcean Spaces bucket in production: the request
// never errored, never got retried, and never completed.
func TestTimeoutDoesNotBoundSlowTrickle(t *testing.T) {
	const (
		configuredTimeout = 300 * time.Millisecond
		dribbleInterval   = 150 * time.Millisecond // < configuredTimeout: nudges the deadline every time
		dribbleBytes      = 20                     // total stall time = dribbleBytes * dribbleInterval = 3s
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Minimal request read: just enough to reach the end of headers so the
		// client's write side is unblocked. Real HTTP parsing isn't needed for
		// this repro -- only the response side is under test.
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil || line == "\r\n" {
				break
			}
		}

		body := fmt.Sprintf("Content-Length: %d\r\n\r\n", dribbleBytes)
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n" + body))

		// Drip one body byte at a time, slower than a real transfer but each
		// gap individually shorter than configuredTimeout. Every successful
		// Write on the CLIENT side (and every Read here) nudges timeoutConn's
		// deadline forward, so neither side should ever see a timeout error --
		// even though the whole exchange takes 10x configuredTimeout.
		for i := 0; i < dribbleBytes; i++ {
			time.Sleep(dribbleInterval)
			if _, err := conn.Write([]byte("x")); err != nil {
				return
			}
		}
	}()

	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	ci.Timeout = fs.Duration(configuredTimeout)
	defer func() { ci.Timeout = fs.Duration(0) }() // don't leak into other tests sharing globalConfig

	client := NewClient(ctx)

	start := time.Now()
	resp, err := client.Get("http://" + ln.Addr().String() + "/")
	require.NoError(t, err, "request errored -- --timeout DID bound total duration (repro did not reproduce the bug)")
	defer resp.Body.Close()

	// client.Get only waits for the response HEADERS. The body -- where the
	// dribble actually happens -- is read separately, exactly as any real
	// HTTP client (including the AWS SDK reading an UploadPart response, or
	// streaming an object body) would.
	_, err = io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	require.NoError(t, err, "body read errored -- --timeout DID bound total duration (repro did not reproduce the bug)")

	minStallTime := time.Duration(dribbleBytes) * dribbleInterval
	require.GreaterOrEqual(t, elapsed, minStallTime,
		"request completed suspiciously fast -- dribble loop may not have run")
	require.Greater(t, elapsed, configuredTimeout*5,
		"request took %s, only %s over the configured --timeout of %s -- expected it to run ~%s over, "+
			"proving --timeout never fired despite the connection making no real progress",
		elapsed, elapsed-configuredTimeout, configuredTimeout, minStallTime)

	t.Logf("configured --timeout=%s; request actually took %s (%.1fx over) and SUCCEEDED with no error, "+
		"no retry, and no --low-level-retries engagement -- --timeout does not bound a slow trickle",
		configuredTimeout, elapsed, float64(elapsed)/float64(configuredTimeout))
}

// TestMinBandwidthDetectsSlowTrickle is the fix side of the repro above: the
// same dribble ("alive" but making no real progress) against the SAME
// --timeout, but now with --min-bandwidth set. Where the previous test
// succeeded 10x over the configured timeout, this one must now fail --
// quickly, on the first window that doesn't clear the minimum -- and hand
// back a real error that feeds --retries / --low-level-retries, rather than
// letting the request run indefinitely.
func TestMinBandwidthDetectsSlowTrickle(t *testing.T) {
	const (
		configuredTimeout = 300 * time.Millisecond
		dribbleInterval   = 150 * time.Millisecond
		dribbleBytes      = 20 // would take dribbleBytes*dribbleInterval = 3s if it ran to completion
		// 1 byte/150ms averages ~6.7 bytes/sec; require far more than that so
		// the very first --timeout window (300ms, at most 2 dribbled bytes)
		// fails the check.
		minBandwidth = 100 // bytes/sec
	)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil || line == "\r\n" {
				break
			}
		}

		body := fmt.Sprintf("Content-Length: %d\r\n\r\n", dribbleBytes)
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n" + body))

		for i := 0; i < dribbleBytes; i++ {
			time.Sleep(dribbleInterval)
			if _, err := conn.Write([]byte("x")); err != nil {
				return // client gave up -- expected once the fix kicks in
			}
		}
	}()

	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	ci.Timeout = fs.Duration(configuredTimeout)
	ci.MinBandwidth = fs.SizeSuffix(minBandwidth)
	defer func() {
		ci.Timeout = fs.Duration(0)
		ci.MinBandwidth = fs.SizeSuffix(0)
	}()

	client := NewClient(ctx)

	start := time.Now()
	resp, err := client.Get("http://" + ln.Addr().String() + "/")
	if err == nil {
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
	}
	elapsed := time.Since(start)

	require.Error(t, err, "request succeeded despite --min-bandwidth=%d/s against a ~6.7 bytes/sec trickle "+
		"-- the fix did not detect the stall", minBandwidth)

	fullDribbleTime := time.Duration(dribbleBytes) * dribbleInterval
	require.Less(t, elapsed, fullDribbleTime,
		"errored, but only after %s -- as long as letting the dribble run to completion (%s). "+
			"Expected detection within roughly one --timeout window.", elapsed, fullDribbleTime)

	t.Logf("configured --timeout=%s --min-bandwidth=%d/s; request FAILED after %s (vs %s for the "+
		"full dribble to complete) with: %v -- stall detected and surfaced as a real error",
		configuredTimeout, minBandwidth, elapsed, fullDribbleTime, err)

	<-serverDone // let the server goroutine's Write see the closed conn and exit before the test does
}
