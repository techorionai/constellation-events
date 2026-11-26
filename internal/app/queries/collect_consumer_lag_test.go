package queries_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-events/internal/app/queries"
	"github.com/techorionai/constellation-events/internal/ports/streams"
	"github.com/techorionai/constellation-events/resources/test/mock"
)

type fake struct {
	groups map[string][]string
	lags   map[string]time.Duration
	// lagErr and groupsErr key: stream or stream/group.
	lagErr    map[string]error
	groupsErr map[string]error
}

func (f fake) apps() *mock.Registrations {
	return &mock.Registrations{ListAppsFunc: func(context.Context) ([]string, error) {
		var apps []string
		for _, a := range []string{"alpha", "beta"} {
			if _, ok := f.groups[a]; ok {
				apps = append(apps, a)
			}
		}
		return apps, nil
	}}
}

func (f fake) streams() *mock.Streams {
	return &mock.Streams{
		GroupsFunc: func(_ context.Context, app string) ([]string, error) {
			if err := f.groupsErr[app]; err != nil {
				return nil, err
			}
			return f.groups[app], nil
		},
		LagFunc: func(_ context.Context, app, group string) (time.Duration, error) {
			if err := f.lagErr[app+"/"+group]; err != nil {
				return 0, err
			}
			return f.lags[app+"/"+group], nil
		},
	}
}

func run(t *testing.T, apps *mock.Registrations, s *mock.Streams) queries.CollectConsumerLagResult {
	t.Helper()
	res, err := queries.NewCollectConsumerLag(apps, s).Execute(context.Background(), queries.CollectConsumerLag{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// U1: 2 apps and 3 groups give 3 values in the order of stream and group.
func TestU1_CollectValues(t *testing.T) {
	f := fake{
		groups: map[string][]string{"alpha": {"a1", "a2"}, "beta": {"b1"}},
		lags:   map[string]time.Duration{"alpha/a1": time.Second, "alpha/a2": 2 * time.Second, "beta/b1": 3 * time.Second},
	}
	res := run(t, f.apps(), f.streams())
	if len(res.Values) != 3 || res.GroupsFound != 3 || len(res.Failures) != 0 {
		t.Fatalf("result = %+v", res)
	}
	want := []queries.LagValue{{"alpha", "a1", time.Second}, {"alpha", "a2", 2 * time.Second}, {"beta", "b1", 3 * time.Second}}
	for i, v := range res.Values {
		if v != want[i] {
			t.Fatalf("value %d = %+v, want %+v", i, v, want[i])
		}
	}
}

// U2: ErrNoGroup for one of 3 groups gives 2 values and no failure.
func TestU2_NoGroupIsNotAFailure(t *testing.T) {
	f := fake{
		groups: map[string][]string{"alpha": {"a1", "a2", "a3"}},
		lagErr: map[string]error{"alpha/a2": fmt.Errorf("wrap: %w", streams.ErrNoGroup)},
	}
	res := run(t, f.apps(), f.streams())
	if len(res.Values) != 2 || len(res.Failures) != 0 {
		t.Fatalf("result = %+v", res)
	}
}

// U3: 1001 groups on 2 streams give the first 1000 values and a count of 1001.
func TestU3_Cap(t *testing.T) {
	a, b := make([]string, 600), make([]string, 50)
	for i := range a {
		a[i] = fmt.Sprintf("g%04d", i)
	}
	for i := range b {
		b[i] = fmt.Sprintf("g%04d", i)
	}
	f := fake{groups: map[string][]string{"alpha": a, "beta": b}}
	s := f.streams()
	res := run(t, f.apps(), s)
	if len(res.Values) != 150 || res.GroupsFound != 650 || res.GroupsCapped != 500 {
		t.Fatalf("values = %d, found = %d, capped = %d", len(res.Values), res.GroupsFound, res.GroupsCapped)
	}
	if res.Values[99].Stream != "alpha" || res.Values[100].Stream != "beta" {
		t.Fatalf("the cap of one stream must not hide the next stream: %+v %+v", res.Values[99], res.Values[100])
	}
	if s.Calls() != 2+150 {
		t.Fatalf("calls = %d, want 152", s.Calls())
	}
}

// U3b: the total cap of 1000 groups holds for many streams.
func TestU3b_TotalCap(t *testing.T) {
	var apps []string
	groups := make([]string, queries.MaxLagGroupsPerStream)
	for i := range groups {
		groups[i] = fmt.Sprintf("g%03d", i)
	}
	for i := range 12 {
		apps = append(apps, fmt.Sprintf("app%02d", i))
	}
	reg := &mock.Registrations{ListAppsFunc: func(context.Context) ([]string, error) { return apps, nil }}
	s := &mock.Streams{
		GroupsFunc: func(context.Context, string) ([]string, error) { return groups, nil },
		LagFunc:    func(context.Context, string, string) (time.Duration, error) { return time.Second, nil },
	}
	res := run(t, reg, s)
	if len(res.Values) != queries.MaxLagGroups || res.GroupsFound != 1200 || res.GroupsCapped != 200 {
		t.Fatalf("values = %d, found = %d, capped = %d", len(res.Values), res.GroupsFound, res.GroupsCapped)
	}
}

// U14: a group name that is not valid UTF-8 or is too long gets no Lag call.
func TestU14_SkipBadNames(t *testing.T) {
	long := strings.Repeat("x", queries.MaxGroupNameBytes+1)
	f := fake{groups: map[string][]string{"alpha": {"bad\xff", long, "good", strings.Repeat("y", queries.MaxGroupNameBytes)}}}
	s := f.streams()
	res := run(t, f.apps(), s)
	if len(res.Values) != 2 || res.GroupsSkipped != 2 || res.GroupsFound != 4 || len(res.Failures) != 0 {
		t.Fatalf("result = %+v", res)
	}
	if s.Calls() != 1+2 {
		t.Fatalf("calls = %d, want 3", s.Calls())
	}
}

// U4: failures of Groups and Lag are in the result and the other values stay.
func TestU4_Failures(t *testing.T) {
	boom := errors.New("boom")
	f := fake{
		groups:    map[string][]string{"alpha": {"a1"}, "beta": {"b1", "b2"}},
		groupsErr: map[string]error{"alpha": boom},
		lagErr:    map[string]error{"beta/b1": boom},
	}
	res := run(t, f.apps(), f.streams())
	if len(res.Values) != 1 || res.Values[0].Group != "b2" || len(res.Failures) != 2 {
		t.Fatalf("result = %+v", res)
	}
	g, l := res.Failures[0], res.Failures[1]
	if g.Operation != queries.OperationGroups || g.Stream != "alpha" || g.Group != "" {
		t.Fatalf("groups failure = %+v", g)
	}
	if l.Operation != queries.OperationLag || l.Stream != "beta" || l.Group != "b1" || !errors.Is(l.Err, boom) {
		t.Fatalf("lag failure = %+v", l)
	}
}

// U5: ListApps fails: one failure and no call to Streams.
func TestU5_ListAppsFails(t *testing.T) {
	boom := errors.New("boom")
	apps := &mock.Registrations{ListAppsFunc: func(context.Context) ([]string, error) { return nil, boom }}
	s := fake{}.streams()
	res := run(t, apps, s)
	if len(res.Failures) != 1 || res.Failures[0].Operation != queries.OperationApps || s.Calls() != 0 {
		t.Fatalf("result = %+v, calls = %d", res, s.Calls())
	}
}
