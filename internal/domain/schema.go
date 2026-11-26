package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// DataSchemaMessage is the fixed text of the field error of a data schema. The
// text of the compiler is not in any response, because it can show internal
// data.
const DataSchemaMessage = "The data schema is not valid, or it refers to an external resource."

const (
	draft2020      = "https://json-schema.org/draft/2020-12/schema"
	maxSchemaDepth = 32
)

// noLoader refuses each URL, so a $ref cannot read a file or call a server.
type noLoader struct{}

func (noLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("a reference to %q is not allowed", url)
}

// decodeNumbers decodes JSON and keeps the text of each number.
func decodeNumbers(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("data after the JSON value")
	}
	return v, nil
}

// CheckDataSchema checks one dataSchema. It returns an error for a schema that
// is not a JSON object, that is larger than MaxDataSchemaBytes, that is not
// draft 2020-12, that has a $id, that has a reference that is not local, that
// is nested deeper than 32 levels, or that does not compile. The compiler
// uses a loader that refuses each URL, so it never reads a file or the
// network. The error text can have internal data. Do not send it to a client.
func CheckDataSchema(typ string, raw json.RawMessage) error {
	if len(raw) > MaxDataSchemaBytes {
		return fmt.Errorf("data_schema_size: %d bytes", len(raw))
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("data_schema_object: %w", err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return errors.New("data_schema_object: must be a JSON object")
	}
	if s, has := obj["$schema"]; has && s != draft2020 {
		return errors.New("data_schema_draft: $schema must be " + draft2020)
	}
	if err := checkKeywords(doc, 1); err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(noLoader{})
	uri := "urn:constellation:event-type:" + typ
	if err := c.AddResource(uri, doc); err != nil {
		return fmt.Errorf("data_schema_compile: %w", err)
	}
	if _, err := c.Compile(uri); err != nil {
		return fmt.Errorf("data_schema_compile: %w", err)
	}
	return nil
}

// Keywords whose value is one schema, a list of schemas, or a map of schemas.
// checkKeywords goes only into these keywords. It does not go into const,
// enum, default, examples, or an unknown keyword, because their values are data.
var (
	schemaKeywords     = []string{"items", "additionalItems", "additionalProperties", "unevaluatedItems", "unevaluatedProperties", "contains", "propertyNames", "not", "if", "then", "else", "contentSchema"}
	schemaListKeywords = []string{"allOf", "anyOf", "oneOf", "prefixItems", "items"}
	schemaMapKeywords  = []string{"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
)

// checkKeywords reports a $ref or $dynamicRef that does not start with "#", a
// $id, and a nesting depth of more than 32 levels.
func checkKeywords(v any, depth int) error {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if depth > maxSchemaDepth {
		return fmt.Errorf("data_schema_depth: more than %d levels", maxSchemaDepth)
	}
	if _, has := m["$id"]; has {
		return errors.New("data_schema_id: $id must not be set")
	}
	for _, k := range []string{"$ref", "$dynamicRef"} {
		if s, ok := m[k].(string); ok && !strings.HasPrefix(s, "#") {
			return fmt.Errorf("data_schema_ref: %s must be a local reference that starts with #", k)
		}
	}
	for _, k := range schemaKeywords {
		if err := checkKeywords(m[k], depth+1); err != nil {
			return err
		}
	}
	for _, k := range schemaListKeywords {
		if list, ok := m[k].([]any); ok {
			for _, item := range list {
				if err := checkKeywords(item, depth+1); err != nil {
					return err
				}
			}
		}
	}
	for _, k := range schemaMapKeywords {
		if sub, ok := m[k].(map[string]any); ok {
			for _, item := range sub {
				if err := checkKeywords(item, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
