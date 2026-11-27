// Package postgres is the PostgreSQL adapter of the event catalog store.
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"

	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
)

// Registrations is the PostgreSQL repository of the registrations and the
// event types.
type Registrations struct {
	pool *pgxpool.Pool
}

var _ registrations.Repository = (*Registrations)(nil)

// New makes the repository.
func New(pool *pgxpool.Pool) *Registrations { return &Registrations{pool: pool} }

// unavailableCode tells whether a PostgreSQL error code means that the store
// cannot serve requests now. The classes are 08 (connection exception), 53
// (insufficient resources), and 57P (operator intervention: shutdown, crash
// shutdown, cannot connect now).
func unavailableCode(code string) bool {
	return strings.HasPrefix(code, "08") || strings.HasPrefix(code, "53") ||
		code == "57P01" || code == "57P02" || code == "57P03"
}

// wrap maps an error of the driver. An error that has no PostgreSQL error code
// (a closed connection, a refused connection, a timeout) is an unavailable
// store. An error with a code of class 08, class 53, or 57P01 to 57P03 is also
// an unavailable store. Each other error with a code is a failure of the
// service.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && !unavailableCode(pgErr.Code) {
		return fmt.Errorf("%s: %w", op, err)
	}
	return fmt.Errorf("%s: %w: %w", op, registrations.ErrUnavailable, err)
}

// Unavailable tells whether err means that the store cannot serve requests. An
// error with no PostgreSQL error code (a closed connection, a refused
// connection, a timeout) or with a code of class 08, class 53, or 57P01 to
// 57P03 means an unavailable store.
func (r *Registrations) Unavailable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return unavailableCode(pgErr.Code)
	}
	return true
}

// Register stores a registration. It joins the transaction in the context. The
// caller starts the transaction and commits it. The statements insert the row
// of the app if it does not exist, then lock it. A second registration of the
// same app waits for the lock and sees the result of the first one. The result
// has the names of the added, removed, and modified types.
func (r *Registrations) Register(ctx context.Context, reg domain.Registration, hash []byte, registeredBy string, now time.Time) (domain.Result, error) {
	now = now.UTC().Truncate(time.Microsecond)
	tx := clientpg.Conn(ctx, r.pool)

	// A row with version 0 is a placeholder. It is visible only in this
	// transaction, because the transaction either replaces it or rolls back.
	if _, err := tx.Exec(ctx, `
		INSERT INTO registrations (app, source, version, hash, registered_at, registered_by)
		VALUES ($1, $2, 0, '', $3, $4) ON CONFLICT (app) DO NOTHING`, reg.App, reg.Source, now, registeredBy); err != nil {
		return domain.Result{}, wrap("insert registration", err)
	}
	var (
		version      int
		storedHash   []byte
		registeredAt time.Time
	)
	if err := tx.QueryRow(ctx,
		`SELECT version, hash, registered_at FROM registrations WHERE app = $1 FOR UPDATE`, reg.App).
		Scan(&version, &storedHash, &registeredAt); err != nil {
		return domain.Result{}, wrap("lock registration", err)
	}
	if version > 0 && bytes.Equal(storedHash, hash) {
		count, err := countTypes(ctx, tx, reg.App)
		if err != nil {
			return domain.Result{}, err
		}
		return domain.Result{App: reg.App, Version: version, RegisteredAt: registeredAt.UTC(), EventTypeCount: count}, nil
	}

	names := make([]string, 0, len(reg.EventTypes))
	for _, t := range reg.EventTypes {
		names = append(names, t.Type)
	}
	var others int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM event_types WHERE app = $1 AND NOT (type = ANY($2))`, reg.App, names).Scan(&others); err != nil {
		return domain.Result{}, wrap("count types", err)
	}
	if others+len(names) > domain.MaxTypesPerApp {
		return domain.Result{}, registrations.ErrTooManyTypes
	}

	res := domain.Result{App: reg.App, Changed: true}
	for _, t := range reg.EventTypes {
		var inserted bool
		// xmax is 0 in the row version that an INSERT makes. An update of an
		// existing row (ON CONFLICT DO UPDATE) sets xmax to the ID of the
		// updating transaction. The expression tells a new type from a changed
		// type. It uses an internal detail of PostgreSQL, and the integration
		// tests (I1 to I4) check it.
		err := tx.QueryRow(ctx, `
			INSERT INTO event_types (type, app, description, data_schema, deprecated, created_at, updated_at, removed_at)
			VALUES ($1, $2, $3, $4::jsonb, $5, $6, $6, NULL)
			ON CONFLICT (type) DO UPDATE SET
			    description = EXCLUDED.description,
			    data_schema = EXCLUDED.data_schema,
			    deprecated  = EXCLUDED.deprecated,
			    removed_at  = NULL,
			    updated_at  = EXCLUDED.updated_at
			WHERE (event_types.description, event_types.deprecated, event_types.data_schema, event_types.removed_at IS NULL)
			      IS DISTINCT FROM (EXCLUDED.description, EXCLUDED.deprecated, EXCLUDED.data_schema, true)
			RETURNING (xmax = 0)`,
			t.Type, reg.App, t.Description, []byte(t.DataSchema), t.Deprecated, now).Scan(&inserted)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			// The type did not change.
		case err != nil:
			return domain.Result{}, wrap("upsert type", err)
		case inserted:
			res.Added++
			res.AddedTypes = append(res.AddedTypes, t.Type)
		default:
			res.Modified++
			res.ModifiedTypes = append(res.ModifiedTypes, t.Type)
		}
	}
	rows, err := tx.Query(ctx, `
		UPDATE event_types SET removed_at = $3, updated_at = $3
		WHERE app = $1 AND removed_at IS NULL AND NOT (type = ANY($2))
		RETURNING type`, reg.App, names, now)
	if err != nil {
		return domain.Result{}, wrap("remove types", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return domain.Result{}, wrap("scan removed type", err)
		}
		res.RemovedTypes = append(res.RemovedTypes, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return domain.Result{}, wrap("remove types", err)
	}
	res.Removed = len(res.RemovedTypes)
	sort.Strings(res.AddedTypes)
	sort.Strings(res.RemovedTypes)
	sort.Strings(res.ModifiedTypes)

	res.Version = version + 1
	res.RegisteredAt = now
	if _, err := tx.Exec(ctx, `
		UPDATE registrations
		SET version = $2, hash = $3, source = $4, registered_at = $5, registered_by = $6
		WHERE app = $1`, reg.App, res.Version, hash, reg.Source, now, registeredBy); err != nil {
		return domain.Result{}, wrap("update registration", err)
	}
	if res.EventTypeCount, err = countTypes(ctx, tx, reg.App); err != nil {
		return domain.Result{}, err
	}
	return res, nil
}

func countTypes(ctx context.Context, tx clientpg.Querier, app string) (int, error) {
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM event_types WHERE app = $1 AND removed_at IS NULL`, app).Scan(&n); err != nil {
		return 0, wrap("count types", err)
	}
	return n, nil
}

const typeColumns = `t.type, t.app, r.source, t.description, t.data_schema, t.deprecated, t.created_at, t.updated_at, t.removed_at`

func scanType(row pgx.Row) (domain.StoredType, error) {
	var (
		t      domain.StoredType
		schema []byte
	)
	if err := row.Scan(&t.Type, &t.App, &t.Source, &t.Description, &schema, &t.Deprecated, &t.CreatedAt, &t.UpdatedAt, &t.RemovedAt); err != nil {
		return domain.StoredType{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(schema))
	dec.UseNumber()
	if err := dec.Decode(&t.DataSchema); err != nil {
		return domain.StoredType{}, fmt.Errorf("decode data_schema of %s: %w", t.Type, err)
	}
	t.CreatedAt, t.UpdatedAt = t.CreatedAt.UTC(), t.UpdatedAt.UTC()
	if t.RemovedAt != nil {
		v := t.RemovedAt.UTC()
		t.RemovedAt = &v
	}
	return t, nil
}

// ListTypes returns at most limit types in the order of the type.
func (r *Registrations) ListTypes(ctx context.Context, f registrations.TypeFilter, limit int) ([]domain.StoredType, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+typeColumns+`
		FROM event_types t JOIN registrations r ON r.app = t.app
		WHERE ($1 = '' OR t.app = $1)
		  AND ($2 OR t.removed_at IS NULL)
		  AND ($3 = '' OR t.type > $3)
		ORDER BY t.type
		LIMIT $4`, f.App, f.IncludeRemoved, f.After, limit)
	if err != nil {
		return nil, wrap("list types", err)
	}
	defer rows.Close()
	var out []domain.StoredType
	for rows.Next() {
		t, err := scanType(rows)
		if err != nil {
			return nil, wrap("scan type", err)
		}
		out = append(out, t)
	}
	return out, wrap("list types", rows.Err())
}

// GetType returns one type, also a removed one.
func (r *Registrations) GetType(ctx context.Context, typ string) (domain.StoredType, error) {
	t, err := scanType(r.pool.QueryRow(ctx, `
		SELECT `+typeColumns+`
		FROM event_types t JOIN registrations r ON r.app = t.app
		WHERE t.type = $1`, typ))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredType{}, registrations.ErrNotFound
	}
	if err != nil {
		return domain.StoredType{}, wrap("get type", err)
	}
	return t, nil
}

// ListRegistrations returns at most limit registrations after the app.
func (r *Registrations) ListRegistrations(ctx context.Context, after string, limit int) ([]domain.StoredRegistration, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT app, source, version, registered_at FROM registrations
		WHERE version > 0 AND ($1 = '' OR app > $1)
		ORDER BY app LIMIT $2`, after, limit)
	if err != nil {
		return nil, wrap("list registrations", err)
	}
	defer rows.Close()
	var out []domain.StoredRegistration
	for rows.Next() {
		var s domain.StoredRegistration
		if err := rows.Scan(&s.App, &s.Source, &s.Version, &s.RegisteredAt); err != nil {
			return nil, wrap("scan registration", err)
		}
		s.RegisteredAt = s.RegisteredAt.UTC()
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("list registrations", err)
	}
	rows.Close()
	if len(out) == 0 {
		return nil, nil
	}
	apps := make([]string, len(out))
	for i := range out {
		apps[i] = out[i].App
	}
	trows, err := r.pool.Query(ctx, `
		SELECT `+typeColumns+`
		FROM event_types t JOIN registrations r ON r.app = t.app
		WHERE t.app = ANY($1) AND t.removed_at IS NULL
		ORDER BY t.app, t.type`, apps)
	if err != nil {
		return nil, wrap("list registration types", err)
	}
	defer trows.Close()
	index := make(map[string]int, len(out))
	for i := range out {
		index[out[i].App] = i
	}
	for trows.Next() {
		t, err := scanType(trows)
		if err != nil {
			return nil, wrap("scan type", err)
		}
		i := index[t.App]
		out[i].Types = append(out[i].Types, t)
	}
	if err := trows.Err(); err != nil {
		return nil, wrap("list registration types", err)
	}
	return out, nil
}

// GetRegistration returns the registration of one app.
func (r *Registrations) GetRegistration(ctx context.Context, app string) (domain.StoredRegistration, error) {
	var s domain.StoredRegistration
	err := r.pool.QueryRow(ctx,
		`SELECT app, source, version, registered_at FROM registrations WHERE app = $1 AND version > 0`, app).
		Scan(&s.App, &s.Source, &s.Version, &s.RegisteredAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StoredRegistration{}, registrations.ErrNotFound
	}
	if err != nil {
		return domain.StoredRegistration{}, wrap("get registration", err)
	}
	s.RegisteredAt = s.RegisteredAt.UTC()
	rows, err := r.pool.Query(ctx, `
		SELECT `+typeColumns+`
		FROM event_types t JOIN registrations r ON r.app = t.app
		WHERE t.app = $1 AND t.removed_at IS NULL ORDER BY t.type`, app)
	if err != nil {
		return domain.StoredRegistration{}, wrap("get registration types", err)
	}
	defer rows.Close()
	for rows.Next() {
		t, err := scanType(rows)
		if err != nil {
			return domain.StoredRegistration{}, wrap("scan type", err)
		}
		s.Types = append(s.Types, t)
	}
	return s, wrap("get registration types", rows.Err())
}

// ListApps returns the app of each registration, sorted.
func (r *Registrations) ListApps(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT app FROM registrations ORDER BY app`)
	if err != nil {
		return nil, wrap("list apps", err)
	}
	defer rows.Close()
	apps := []string{}
	for rows.Next() {
		var app string
		if err := rows.Scan(&app); err != nil {
			return nil, wrap("scan app", err)
		}
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, wrap("list apps", err)
	}
	return apps, nil
}
