package domain_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/techorionai/constellation-platform-go/eventtypes"

	"github.com/techorionai/constellation-events/internal/domain"
)

func def(typ, desc, schema string) domain.EventTypeDefinition {
	return domain.EventTypeDefinition{Type: typ, Description: desc, DataSchema: json.RawMessage(schema)}
}

func reg(app string, types ...domain.EventTypeDefinition) domain.Registration {
	return domain.Registration{Source: "/" + app, App: app, EventTypes: types}
}

// codes returns the field and the code of each error, as "field code".
func codes(errs []domain.FieldError) []string {
	out := make([]string, 0, len(errs))
	for _, e := range errs {
		out = append(out, e.Field+" "+e.Code)
	}
	return out
}

func hasError(errs []domain.FieldError, field, code string) bool {
	for _, e := range errs {
		if e.Field == field && e.Code == code {
			return true
		}
	}
	return false
}

// bigSchema makes a schema of exactly n bytes.
func bigSchema(n int) string {
	prefix := `{"description":"`
	suffix := `"}`
	return prefix + strings.Repeat("a", n-len(prefix)-len(suffix)) + suffix
}

// U1: a valid registration of 200 types, a description of 1024 code points
// with multi-byte characters, and a schema of exactly 64 KiB.
func TestU1_ValidRegistration(t *testing.T) {
	var types []domain.EventTypeDefinition
	for i := range 200 {
		types = append(types, def(fmt.Sprintf("helpdesk.ticket.t%d", i), "A ticket event.", `{"type":"object"}`))
	}
	types[0].Description = strings.Repeat("é", 1024)
	types[1].DataSchema = json.RawMessage(bigSchema(domain.MaxDataSchemaBytes))
	if len(types[1].DataSchema) != 64<<10 {
		t.Fatalf("schema size = %d", len(types[1].DataSchema))
	}
	if errs := reg("helpdesk", types...).Validate(); len(errs) != 0 {
		t.Fatalf("errors: %v", codes(errs))
	}
}

// U2: each bad rule gives its code and its JSON Pointer, and all errors are in
// one answer.
func TestU2_BadRules(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.json")
	if err := os.WriteFile(valid, []byte(`{"type":"string"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(valid)}).String()
	if !strings.HasPrefix(fileURL, "file:///") {
		fileURL = "file:///" + strings.TrimPrefix(fileURL, "file:")
	}
	t.Chdir(dir)
	if err := os.WriteFile("other.json", []byte(`{"type":"string"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ok := `{"type":"object"}`
	cases := []struct {
		name  string
		r     domain.Registration
		field string
		code  string
	}{
		{"source", domain.Registration{Source: "/other", App: "helpdesk", EventTypes: []domain.EventTypeDefinition{def("helpdesk.ticket.created", "d", ok)}}, "/source", domain.CodeSourceMismatch},
		{"type of another app", reg("helpdesk", def("crm.ticket.created", "d", ok)), "/eventTypes/0/type", domain.CodeEventTypeAppMismatch},
		{"help prefix", reg("helpdesk", def("help.ticket.created", "d", ok)), "/eventTypes/0/type", domain.CodeEventTypeAppMismatch},
		{"duplicate", reg("helpdesk", def("helpdesk.ticket.created", "d", ok), def("helpdesk.ticket.created", "d", ok)), "/eventTypes/1/type", domain.CodeDuplicateEventType},
		{"no types", reg("helpdesk"), "/eventTypes", domain.CodeLimitExceeded},
		{"description 0", reg("helpdesk", def("helpdesk.ticket.created", "", ok)), "/eventTypes/0/description", domain.CodeLimitExceeded},
		{"description 1025", reg("helpdesk", def("helpdesk.ticket.created", strings.Repeat("é", 1025), ok)), "/eventTypes/0/description", domain.CodeLimitExceeded},
		{"schema 64 KiB plus 1", reg("helpdesk", def("helpdesk.ticket.created", "d", bigSchema(64<<10+1))), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"schema does not compile", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"type": 5}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"file ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"`+fileURL+`"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"http ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"http://example.com/s.json"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"https ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"https://example.com/s.json"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"relative ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"other.json"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"nested ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"properties":{"a":{"items":{"$ref":"other.json"}}}}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"draft 7", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$schema":"http://json-schema.org/draft-07/schema#"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"id", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$id":"urn:x"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"not an object", reg("helpdesk", def("helpdesk.ticket.created", "d", `[1]`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"back reference", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"pattern":"(a)\\1"}`)), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
		{"deep", reg("helpdesk", def("helpdesk.ticket.created", "d", strings.Repeat(`{"not":`, 40)+`{}`+strings.Repeat(`}`, 40))), "/eventTypes/0/dataSchema", domain.CodeInvalidDataSchema},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := tc.r.Validate()
			if !hasError(errs, tc.field, tc.code) {
				t.Fatalf("want %s %s, have %v", tc.field, tc.code, codes(errs))
			}
			for _, e := range errs {
				if e.Code == domain.CodeInvalidDataSchema && e.Message != domain.DataSchemaMessage {
					t.Fatalf("data schema message = %q", e.Message)
				}
			}
		})
	}

	t.Run("201 types", func(t *testing.T) {
		var types []domain.EventTypeDefinition
		for i := range 201 {
			types = append(types, def(fmt.Sprintf("helpdesk.ticket.t%d", i), "d", ok))
		}
		if errs := reg("helpdesk", types...).Validate(); !hasError(errs, "/eventTypes", domain.CodeLimitExceeded) {
			t.Fatalf("errors: %v", codes(errs))
		}
	})

	t.Run("all errors in one answer", func(t *testing.T) {
		r := domain.Registration{Source: "/x", App: "helpdesk", EventTypes: []domain.EventTypeDefinition{
			def("crm.a.b", "", `{"type":5}`), def("crm.a.b", "d", ok),
		}}
		if n := len(r.Validate()); n < 5 {
			t.Fatalf("errors = %d, want 5 or more", n)
		}
	})
}

// U3: the hash is equal for the same content in another form, and different for
// another content.
func TestU3_CanonicalHash(t *testing.T) {
	a := reg("helpdesk",
		def("helpdesk.ticket.created", "A", `{"type":"object","properties":{"a":{"type":"string"},"b":{"maximum":1.0}}}`),
		def("helpdesk.ticket.closed", "B", `{"type":"object"}`))
	b := reg("helpdesk",
		def("helpdesk.ticket.closed", "B", " {\n \"type\" : \"object\" } "),
		def("helpdesk.ticket.created", "A", `{"properties":{"b":{"maximum":1.0},"a":{"type":"string"}},"type":"object"}`))
	deprecatedFalse := b
	deprecatedFalse.EventTypes = append([]domain.EventTypeDefinition(nil), b.EventTypes...)
	deprecatedFalse.EventTypes[0].Deprecated = false

	hash := func(r domain.Registration) string {
		t.Helper()
		h, err := r.Hash()
		if err != nil {
			t.Fatal(err)
		}
		return string(h)
	}
	if hash(a) != hash(b) || hash(a) != hash(deprecatedFalse) {
		t.Fatal("the same content must give the same hash")
	}

	changed := func(f func(r *domain.Registration)) string {
		r := reg("helpdesk", a.EventTypes...)
		r.EventTypes = append([]domain.EventTypeDefinition(nil), a.EventTypes...)
		f(&r)
		return hash(r)
	}
	for name, f := range map[string]func(r *domain.Registration){
		"description": func(r *domain.Registration) { r.EventTypes[0].Description = "other" },
		"deprecated":  func(r *domain.Registration) { r.EventTypes[0].Deprecated = true },
		"number":      func(r *domain.Registration) { r.EventTypes[0].DataSchema = json.RawMessage(`{"maximum":1.5}`) },
		"number text": func(r *domain.Registration) { r.EventTypes[0].DataSchema = json.RawMessage(`{"maximum":1}`) },
	} {
		if changed(f) == hash(a) {
			t.Fatalf("a change of the %s must change the hash", name)
		}
	}
}

// U11: the domain check and Registration.Validate of platform-go v0.6.0 accept
// the same valid registrations and refuse the same bad registrations.
func TestU11_ParityWithPlatformGo(t *testing.T) {
	ok := `{"type":"object","properties":{"id":{"type":"string"}}}`
	type tc struct {
		name string
		r    domain.Registration
	}
	var cases []tc
	add := func(name string, r domain.Registration) { cases = append(cases, tc{name, r}) }

	for i, schema := range []string{
		ok, `{}`, `{"type":"string","maxLength":10}`, `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}`,
		`{"$defs":{"a":{"type":"string"}},"properties":{"x":{"$ref":"#/$defs/a"}}}`, `{"oneOf":[{"type":"string"},{"type":"number"}]}`,
		`{"type":"object","additionalProperties":false,"required":["a"]}`, `{"enum":["a","b"]}`, `{"const":{"$ref":"other.json"}}`,
		`{"type":"integer","maximum":9007199254740993,"multipleOf":0.1}`,
	} {
		add(fmt.Sprintf("valid %d", i), reg("helpdesk", def("helpdesk.ticket.created", "A description.", schema)))
	}
	add("valid two types", reg("helpdesk", def("helpdesk.ticket.created", "d", ok), def("helpdesk.ticket.closed", "d", ok)))

	add("source", domain.Registration{Source: "/x", App: "helpdesk", EventTypes: []domain.EventTypeDefinition{def("helpdesk.ticket.created", "d", ok)}})
	add("other app", reg("helpdesk", def("crm.ticket.created", "d", ok)))
	add("prefix", reg("helpdesk", def("help.ticket.created", "d", ok)))
	add("two segments", reg("helpdesk", def("helpdesk.created", "d", ok)))
	add("upper case", reg("helpdesk", def("helpdesk.Ticket.created", "d", ok)))
	add("duplicate", reg("helpdesk", def("helpdesk.ticket.created", "d", ok), def("helpdesk.ticket.created", "d", ok)))
	add("no types", reg("helpdesk"))
	add("empty description", reg("helpdesk", def("helpdesk.ticket.created", "", ok)))
	add("long description", reg("helpdesk", def("helpdesk.ticket.created", strings.Repeat("a", 1025), ok)))
	add("big schema", reg("helpdesk", def("helpdesk.ticket.created", "d", bigSchema(64<<10+1))))
	add("bad schema", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"type":5}`)))
	add("file ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"file:///etc/passwd"}`)))
	add("http ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"http://example.com/s.json"}`)))
	add("relative ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$ref":"other.json"}`)))
	add("dynamic ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$dynamicRef":"https://example.com/x"}`)))
	add("draft 7", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"$schema":"http://json-schema.org/draft-07/schema#"}`)))
	add("not an object", reg("helpdesk", def("helpdesk.ticket.created", "d", `"x"`)))
	add("bad app", reg("Help-Desk", def("Help-Desk.ticket.created", "d", ok)))
	add("pattern", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"pattern":"("}`)))
	add("nested ref", reg("helpdesk", def("helpdesk.ticket.created", "d", `{"allOf":[{"$ref":"x.json"}]}`)))

	if len(cases) < 30 {
		t.Fatalf("cases = %d, want 30", len(cases))
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ours := len(c.r.Validate()) == 0
			theirs := toPlatform(c.r).Validate() == nil
			if ours != theirs {
				t.Fatalf("service accepts = %v, platform-go accepts = %v", ours, theirs)
			}
			wantValid := strings.HasPrefix(c.name, "valid")
			if ours != wantValid {
				t.Fatalf("accepts = %v, want %v", ours, wantValid)
			}
		})
	}
}

func toPlatform(r domain.Registration) eventtypes.Registration {
	out := eventtypes.Registration{Source: r.Source, App: r.App}
	for _, t := range r.EventTypes {
		out.EventTypes = append(out.EventTypes, eventtypes.EventType{
			Type: t.Type, Description: t.Description, DataSchema: t.DataSchema, Deprecated: t.Deprecated,
		})
	}
	return out
}
