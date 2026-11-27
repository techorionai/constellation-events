package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/techorionai/constellation-events/internal/ports/registrations"
)

// R5: the error classes 08 and 53 and the codes 57P01 to 57P03 mean that the
// store is not available. Another code is a failure of the service.
func TestWrapUnavailableCodes(t *testing.T) {
	cases := map[string]bool{
		"08000": true, "08006": true, "08001": true,
		"53000": true, "53200": true, "53300": true,
		"57P01": true, "57P02": true, "57P03": true,
		"23505": false, "42P01": false, "22P02": false, "57014": false,
	}
	for code, unavailable := range cases {
		err := wrap("op", &pgconn.PgError{Code: code})
		if got := errors.Is(err, registrations.ErrUnavailable); got != unavailable {
			t.Errorf("code %s: unavailable = %v, want %v", code, got, unavailable)
		}
	}
	for name, err := range map[string]error{
		"closed network": net.ErrClosed, "end of stream": io.ErrUnexpectedEOF,
		"timeout": context.DeadlineExceeded, "connect": &pgconn.ConnectError{},
	} {
		if !errors.Is(wrap("op", err), registrations.ErrUnavailable) || !(&Registrations{}).Unavailable(err) {
			t.Errorf("%s must be unavailable", name)
		}
	}
	// R1: an error that is not a failure of the connection is not unavailable.
	for name, err := range map[string]error{
		"canceled": context.Canceled, "scan": errors.New("scan: wrong type"),
	} {
		if errors.Is(wrap("op", err), registrations.ErrUnavailable) || (&Registrations{}).Unavailable(err) {
			t.Errorf("%s must not be unavailable", name)
		}
	}
}
