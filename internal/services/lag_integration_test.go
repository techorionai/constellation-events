//go:build integration

package services_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	valkeygo "github.com/valkey-io/valkey-go"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/techorionai/constellation-events/internal/adapters/postgres"
	"github.com/techorionai/constellation-events/internal/services"
	"github.com/techorionai/constellation-events/resources/test/testserver"
)

const lagInterval = 500 * time.Millisecond

func streamKey(app string) string { return "events.stream:" + app }

// lagTest is the setup of a lag test: a service, a reader of its instruments,
// and an admin client of Valkey.
type lagTest struct {
	t      *testing.T
	srv    *testserver.Server
	reader *sdkmetric.ManualReader
	admin  valkeygo.Client
}

func newLagTest(t *testing.T, opts ...testserver.Option) *lagTest {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	lt := &lagTest{t: t, reader: reader, admin: env.ValkeyAdmin(t)}
	lt.srv = started(t, append([]testserver.Option{testserver.WithMeterProvider(mp)}, opts...)...)
	return lt
}

func (lt *lagTest) do(args ...string) valkeygo.ValkeyResult {
	lt.t.Helper()
	return lt.admin.Do(context.Background(), lt.admin.B().Arbitrary(args[0]).Args(args[1:]...).Build())
}

func (lt *lagTest) mustDo(args ...string) {
	lt.t.Helper()
	if err := lt.do(args...).Error(); err != nil {
		lt.t.Fatalf("%s: %v", strings.Join(args, " "), err)
	}
}

// resetStream deletes the stream of the app, now and at the end of the test.
func (lt *lagTest) resetStream(app string) {
	lt.t.Helper()
	lt.mustDo("DEL", streamKey(app))
	lt.t.Cleanup(func() { _ = lt.do("DEL", streamKey(app)).Error() })
}

// addEvents adds one event for each age, with an explicit stream ID. The ages
// go from the oldest to the newest.
func (lt *lagTest) addEvents(app string, ages ...time.Duration) {
	lt.t.Helper()
	now := time.Now()
	for i, age := range ages {
		id := fmt.Sprintf("%d-0", now.Add(-age).UnixMilli())
		lt.mustDo("XADD", streamKey(app), id, "event", fmt.Sprintf(`{"n":%d}`, i))
	}
}

func (lt *lagTest) createGroup(app, group string) {
	lt.t.Helper()
	lt.mustDo("XGROUP", "CREATE", streamKey(app), group, "0")
}

// readGroup reads count events for the group and acknowledges them if ack is
// true.
func (lt *lagTest) readGroup(app, group string, count int, ack bool) {
	lt.t.Helper()
	res := lt.do("XREADGROUP", "GROUP", group, "consumer1", "COUNT", fmt.Sprint(count), "STREAMS", streamKey(app), ">")
	entries, err := res.AsXRead()
	if err != nil {
		lt.t.Fatalf("XREADGROUP: %v", err)
	}
	if !ack {
		return
	}
	for _, e := range entries[streamKey(app)] {
		lt.mustDo("XACK", streamKey(app), group, e.ID)
	}
}

// metrics is the content of the instruments.
type metrics struct {
	lag       map[string]float64
	collected *int64
	groups    *int64
	errors    map[string]int64
}

func (lt *lagTest) metrics() metrics {
	lt.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := lt.reader.Collect(context.Background(), &rm); err != nil {
		lt.t.Fatal(err)
	}
	m := metrics{lag: map[string]float64{}, errors: map[string]int64{}}
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			switch d := md.Data.(type) {
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					st, _ := p.Attributes.Value("stream")
					gr, _ := p.Attributes.Value("group")
					m.lag[st.AsString()+"/"+gr.AsString()] = p.Value
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					v := p.Value
					if md.Name == "constellation.events.lag.collected" {
						m.collected = &v
					} else {
						m.groups = &v
					}
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					op, _ := p.Attributes.Value("operation")
					m.errors[op.AsString()] += p.Value
				}
			}
		}
	}
	return m
}

// waitFor polls the instruments until the condition is true.
func (lt *lagTest) waitFor(what string, cond func(metrics) bool) metrics {
	lt.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		m := lt.metrics()
		if cond(m) {
			return m
		}
		if time.Now().After(deadline) {
			lt.t.Fatalf("timeout: %s. metrics = %+v", what, m)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// between tells whether the lag of a group is from lo to hi seconds.
func between(key string, lo, hi float64) func(metrics) bool {
	return func(m metrics) bool {
		v, ok := m.lag[key]
		return ok && v >= lo && v <= hi
	}
}

// setupHelpdesk registers the app helpdesk and adds 3 events with the ages of
// 60 s, 40 s, and 20 s. The group audit read and acknowledged one event. The
// group workflows read none.
func (lt *lagTest) setupHelpdesk() {
	lt.t.Helper()
	registerOK(lt.t, lt.srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.created"))
	lt.resetStream("helpdesk")
	lt.addEvents("helpdesk", 60*time.Second, 40*time.Second, 20*time.Second)
	lt.createGroup("helpdesk", "audit")
	lt.createGroup("helpdesk", "workflows")
	lt.readGroup("helpdesk", "audit", 1, true)
}

// I1: the lag of two groups of one stream.
func TestLag_I1_TwoGroups(t *testing.T) {
	lt := newLagTest(t)
	lt.setupHelpdesk()
	m := lt.waitFor("lag of audit and workflows", func(m metrics) bool {
		return between("helpdesk/audit", 40, 55)(m) && between("helpdesk/workflows", 60, 75)(m)
	})
	if m.collected == nil || m.groups == nil || *m.groups != 2 {
		t.Fatalf("collected = %v, groups = %v", m.collected, m.groups)
	}
}

// I2: a group that read all events and acknowledged none has the lag of the
// oldest pending event.
func TestLag_I2_Pending(t *testing.T) {
	lt := newLagTest(t)
	lt.setupHelpdesk()
	lt.createGroup("helpdesk", "crm")
	lt.readGroup("helpdesk", "crm", 10, false)
	lt.waitFor("lag of crm", between("helpdesk/crm", 60, 75))
}

// I3: a group that acknowledged all events has the lag 0.
func TestLag_I3_AllAcknowledged(t *testing.T) {
	lt := newLagTest(t)
	lt.setupHelpdesk()
	lt.waitFor("lag of audit", between("helpdesk/audit", 40, 55))
	lt.readGroup("helpdesk", "audit", 10, true)
	lt.waitFor("lag 0 of audit", between("helpdesk/audit", 0, 0))
}

// I4: a consumer that does not read has a lag that goes up.
func TestLag_I4_StoppedConsumer(t *testing.T) {
	lt := newLagTest(t)
	lt.setupHelpdesk()
	first := lt.waitFor("lag of workflows", between("helpdesk/workflows", 60, 75)).lag["helpdesk/workflows"]
	last := first
	for range 3 {
		prev := last
		m := lt.waitFor("lag goes up", func(m metrics) bool { return m.lag["helpdesk/workflows"] > prev+0.2 })
		last = m.lag["helpdesk/workflows"]
	}
	if last < first+1 {
		t.Fatalf("the lag went from %.2f to %.2f in 3 steps", first, last)
	}
}

// I5: a deleted group is not in the next snapshot and is not an error.
func TestLag_I5_DeletedGroup(t *testing.T) {
	lt := newLagTest(t)
	lt.setupHelpdesk()
	lt.createGroup("helpdesk", "crm")
	lt.waitFor("lag of crm", between("helpdesk/crm", 60, 75))
	before := lt.metrics()
	lt.mustDo("XGROUP", "DESTROY", streamKey("helpdesk"), "crm")
	lt.waitFor("crm is gone", func(m metrics) bool { _, ok := m.lag["helpdesk/crm"]; return !ok })
	time.Sleep(3 * lagInterval)
	after := lt.metrics()
	if len(after.errors) != len(before.errors) {
		t.Fatalf("errors before = %v, after = %v", before.errors, after.errors)
	}
	for op, n := range after.errors {
		if n != before.errors[op] {
			t.Fatalf("errors before = %v, after = %v", before.errors, after.errors)
		}
	}
}

// I6: the collector runs as the Valkey user of the service. The user cannot
// write to the stream, cannot read it with GET, and cannot write a key of
// another service. The ACL rule of platform-go does not refuse SCAN, and the
// collector does not use it, so the test does not check it.
func TestLag_I6_ACL(t *testing.T) {
	lt := newLagTest(t, testserver.WithValkeyACL())
	lt.setupHelpdesk()
	lt.waitFor("lag of workflows", between("helpdesk/workflows", 60, 75))
	if m := lt.metrics(); len(m.errors) != 0 {
		t.Fatalf("errors = %v", m.errors)
	}

	cfg := lt.srv.Config.Valkey
	user, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{cfg.Address}, Username: lt.srv.ValkeyUser, Password: cfg.Password.Reveal(), DisableCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(user.Close)
	for _, args := range [][]string{
		{"XADD", streamKey("helpdesk"), "*", "k", "v"},
		{"GET", streamKey("helpdesk")},
		{"SET", "other:key", "v"},
	} {
		err := user.Do(context.Background(), user.B().Arbitrary(args[0]).Args(args[1:]...).Build()).Error()
		if err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("%s: error = %v, want NOPERM", args[0], err)
		}
	}
}

// I7: without the stream selectors, the collection fails and the old values
// stay. The readiness does not change. After the selectors are back, the
// collection works again.
func TestLag_I7_SelectorsRemoved(t *testing.T) {
	lt := newLagTest(t, testserver.WithValkeyACL())
	lt.setupHelpdesk()
	m := lt.waitFor("lag of workflows", between("helpdesk/workflows", 60, 75))
	collected := *m.collected

	lt.mustDo("ACL", "SETUSER", lt.srv.ValkeyUser, "clearselectors")
	m = lt.waitFor("3 failed collections", func(m metrics) bool { return m.errors["groups"] >= 3 })
	if m.collected == nil || *m.collected < collected {
		t.Fatalf("collected = %v, was %d", m.collected, collected)
	}
	if got := *m.collected; got > collected+3 {
		// The last collection with no failure ended at most one interval after
		// the measure of the first value, because the clear happens at once.
		t.Fatalf("collected = %d, was %d", got, collected)
	}
	frozen := *m.collected
	if _, ok := m.lag["helpdesk/workflows"]; !ok {
		t.Fatalf("the old value of workflows is gone after 3 failures: %v", m.lag)
	}
	if got := lt.srv.Ready(); got != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", got)
	}

	args := append([]string{"ACL", "SETUSER", lt.srv.ValkeyUser}, testserver.ACLSelectors()...)
	lt.mustDo(args...)
	m = lt.waitFor("collection works again", func(m metrics) bool { return m.collected != nil && *m.collected > frozen })
	if v := m.lag["helpdesk/workflows"]; v < 60 || v > 80 {
		t.Fatalf("lag of workflows = %.2f", v)
	}
}

// tcpProxy forwards connections to a target. Stop closes the listener and all
// connections. Start listens again on the same address.
type tcpProxy struct {
	target string
	addr   string
	mu     sync.Mutex
	ln     net.Listener
	conns  []net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	p := &tcpProxy{target: target, addr: "127.0.0.1:0"}
	p.Start(t)
	t.Cleanup(p.Stop)
	return p
}

func (p *tcpProxy) Start(t *testing.T) {
	t.Helper()
	var ln net.Listener
	var err error
	for range 50 {
		if ln, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p.mu.Lock()
	p.ln = ln
	p.addr = ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(context.Background(), "tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
}

func (p *tcpProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

// I8: when PostgreSQL does not answer, the collection fails and the collection
// time does not change. After PostgreSQL is back, it changes again.
func TestLag_I8_DatabaseDown(t *testing.T) {
	db := env.NewDB(t)
	// The service connects through a proxy, so that the test can stop and start
	// the connection and keep the address.
	rest := strings.SplitN(string(db.URL), "@", 2)
	hostPort, path, _ := strings.Cut(rest[1], "/")
	proxy := newTCPProxy(t, hostPort)
	db.URL = platformconfig.Secret(rest[0] + "@" + proxy.addr + "/" + path)

	lt := newLagTest(t, testserver.WithDB(db))
	lt.setupHelpdesk()
	m := lt.waitFor("lag of workflows", between("helpdesk/workflows", 60, 75))
	collected := *m.collected

	proxy.Stop()
	m = lt.waitFor("failed collections", func(m metrics) bool { return m.errors["apps"] >= 3 })
	if m.collected == nil || *m.collected > collected+3 {
		t.Fatalf("collected = %v, was %d", m.collected, collected)
	}
	frozen := *m.collected

	proxy.Start(t)
	lt.waitFor("collection works again", func(m metrics) bool { return m.collected != nil && *m.collected > frozen })
}

// I9: ListApps gives the apps in order.
func TestLag_I9_ListApps(t *testing.T) {
	ctx := context.Background()
	db := env.NewDB(t)
	c, err := clientpg.New(ctx, clientpg.Config{URL: db.URL, Database: db.Name, MaxConns: 2, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	if err := services.RunMigrations(ctx, c); err != nil {
		t.Fatal(err)
	}
	repo := postgres.New(c.Pool)
	apps, err := repo.ListApps(ctx)
	if err != nil || len(apps) != 0 {
		t.Fatalf("apps = %v, error = %v", apps, err)
	}
	for _, app := range []string{"helpdesk", "crm", "audit"} {
		if _, err := c.Pool.Exec(ctx, `INSERT INTO registrations (app, source, version, hash, registered_at, registered_by)
			VALUES ($1, $2, 1, ''::bytea, now(), 'test')`, app, "/"+app); err != nil {
			t.Fatal(err)
		}
	}
	apps, err = repo.ListApps(ctx)
	if err != nil || strings.Join(apps, ",") != "audit,crm,helpdesk" {
		t.Fatalf("apps = %v, error = %v", apps, err)
	}
}
