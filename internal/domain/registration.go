// Package domain holds the rules of the event catalog. It imports the
// standard library and the JSON Schema library only.
package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits of a registration.
const (
	// MaxDataSchemaBytes is the largest dataSchema of one event type.
	MaxDataSchemaBytes = 64 << 10
	// MaxTypesPerRegistration is the largest number of types in one registration.
	MaxTypesPerRegistration = 200
	// MaxTypesPerApp is the largest number of types of one app in the
	// catalog. Removed types count.
	MaxTypesPerApp = 1000
	// MaxDescriptionLength is the largest description, in Unicode code points.
	MaxDescriptionLength = 1024
	// MaxTypeLength is the largest length of an event type.
	MaxTypeLength = 128
	// MaxAppKeyLength is the largest length of an app key.
	MaxAppKeyLength = 32
)

// Codes of the field errors of a registration.
const (
	CodeSourceMismatch       = "source_mismatch"
	CodeEventTypeAppMismatch = "event_type_app_mismatch"
	CodeDuplicateEventType   = "duplicate_event_type"
	CodeLimitExceeded        = "limit_exceeded"
	CodeInvalidDataSchema    = "invalid_data_schema"
)

var (
	typePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2,}$`)
	appKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// ValidAppKey reports whether s is an app key.
func ValidAppKey(s string) bool { return len(s) <= MaxAppKeyLength && appKeyPattern.MatchString(s) }

// ValidEventType reports whether s is an event type.
func ValidEventType(s string) bool { return len(s) <= MaxTypeLength && typePattern.MatchString(s) }

// FieldError is one problem of a registration. Field is a JSON Pointer.
type FieldError struct {
	Field   string
	Code    string
	Message string
}

// EventTypeDefinition is one event type that an app registers.
type EventTypeDefinition struct {
	Type        string
	Description string
	// DataSchema is the JSON text of the schema.
	DataSchema json.RawMessage
	Deprecated bool
}

// Registration is the full set of event types of one app.
type Registration struct {
	Source     string
	App        string
	EventTypes []EventTypeDefinition
}

// Validate checks all content rules and returns all errors. The caller check
// (registration_app_mismatch) is not part of it.
func (r Registration) Validate() []FieldError {
	var errs []FieldError
	add := func(field, code, msg string) { errs = append(errs, FieldError{Field: field, Code: code, Message: msg}) }
	if r.Source != "/"+r.App {
		add("/source", CodeSourceMismatch, "The source must be a slash followed by the app key.")
	}
	if n := len(r.EventTypes); n < 1 || n > MaxTypesPerRegistration {
		add("/eventTypes", CodeLimitExceeded, fmt.Sprintf("A registration has 1 to %d event types.", MaxTypesPerRegistration))
		if n > MaxTypesPerRegistration {
			// Do not check or compile the types of a body that is too large.
			// Each compile costs CPU, and the body limit is the only other limit.
			return errs
		}
	}
	seen := make(map[string]struct{}, len(r.EventTypes))
	for i, t := range r.EventTypes {
		base := fmt.Sprintf("/eventTypes/%d", i)
		switch {
		case !ValidEventType(t.Type):
			add(base+"/type", CodeEventTypeAppMismatch, "The type is not a valid event type.")
		case !strings.HasPrefix(t.Type, r.App+"."):
			add(base+"/type", CodeEventTypeAppMismatch, "The type must start with the app key and a dot.")
		}
		if _, dup := seen[t.Type]; dup {
			add(base+"/type", CodeDuplicateEventType, "The type occurs more than one time.")
		}
		seen[t.Type] = struct{}{}
		if n := utf8.RuneCountInString(t.Description); n < 1 || n > MaxDescriptionLength {
			add(base+"/description", CodeLimitExceeded, fmt.Sprintf("The description has 1 to %d characters.", MaxDescriptionLength))
		}
		if err := CheckDataSchema(t.Type, t.DataSchema); err != nil {
			add(base+"/dataSchema", CodeInvalidDataSchema, DataSchemaMessage)
		}
	}
	return errs
}

// canonicalType is the part of a type that the hash covers.
type canonicalType struct {
	DataSchema  any    `json:"dataSchema"`
	Deprecated  bool   `json:"deprecated"`
	Description string `json:"description"`
	Type        string `json:"type"`
}

// Hash returns the SHA-256 of the canonical form of the registration. The
// form has the app, the source, and the types in the order of the type. The
// keys of each dataSchema are sorted, and the number texts stay.
func (r Registration) Hash() ([]byte, error) {
	types := make([]canonicalType, 0, len(r.EventTypes))
	for _, t := range r.EventTypes {
		schema, err := decodeNumbers(t.DataSchema)
		if err != nil {
			return nil, fmt.Errorf("canonical form of %s: %w", t.Type, err)
		}
		types = append(types, canonicalType{DataSchema: schema, Deprecated: t.Deprecated, Description: t.Description, Type: t.Type})
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Type < types[j].Type })
	b, err := json.Marshal(struct {
		App        string          `json:"app"`
		EventTypes []canonicalType `json:"eventTypes"`
		Source     string          `json:"source"`
	}{App: r.App, EventTypes: types, Source: r.Source})
	if err != nil {
		return nil, fmt.Errorf("canonical form: %w", err)
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

// StoredType is one event type of the catalog.
type StoredType struct {
	Type        string
	App         string
	Source      string
	Description string
	// DataSchema is the decoded schema. Numbers are json.Number values.
	DataSchema map[string]any
	Deprecated bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
	RemovedAt  *time.Time
}

// StoredRegistration is the registration of one app with its types that have
// no removal time.
type StoredRegistration struct {
	App          string
	Source       string
	Version      int
	RegisteredAt time.Time
	Types        []StoredType
}

// Result is the answer to a registration.
type Result struct {
	App            string
	Version        int
	RegisteredAt   time.Time
	EventTypeCount int
	// Changed is true when the registration made a new version.
	Changed bool
	// Added, Removed, and Modified count the types that the registration changed.
	Added, Removed, Modified int
	// AddedTypes, RemovedTypes, and ModifiedTypes have the names of the types
	// that the registration changed, in the order of the type.
	AddedTypes, RemovedTypes, ModifiedTypes []string
}

// ApproxSize returns the size in bytes that the type has in a response body.
func (t StoredType) ApproxSize() int {
	return len(t.Type) + len(t.App) + len(t.Source) + len(t.Description) + schemaSize(t.DataSchema) + 300
}

func schemaSize(m map[string]any) int {
	b, err := json.Marshal(m)
	if err != nil {
		return 0
	}
	return len(b)
}

// ApproxSize returns the size in bytes that the registration has in a response body.
func (r StoredRegistration) ApproxSize() int {
	n := len(r.App) + len(r.Source) + 200
	for _, t := range r.Types {
		n += len(t.Type) + len(t.Description) + schemaSize(t.DataSchema) + 100
	}
	return n
}
