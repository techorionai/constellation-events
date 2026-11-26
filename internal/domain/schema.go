package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
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

// schemaWalk holds the state of one check of a document.
type schemaWalk struct {
	// pointers is the set of JSON Pointers of the subschemas that the walk
	// examined. The root is the empty string.
	pointers map[string]struct{}
	// anchors is the set of the $anchor and $dynamicAnchor names of the
	// examined subschemas.
	anchors map[string]struct{}
	// refs is the list of the $ref and $dynamicRef values.
	refs []string
}

// checkKeywords reports a $ref or $dynamicRef that does not start with "#", a
// $id, a nesting depth of more than 32 levels, and a local reference whose
// target the walk did not examine. A target that the walk did not examine can
// be a part of an unknown keyword. The compiler compiles it as a schema, with
// no check of the $id and the depth.
func checkKeywords(v any, depth int) error {
	w := &schemaWalk{pointers: map[string]struct{}{}, anchors: map[string]struct{}{}}
	if err := w.walk(v, depth, ""); err != nil {
		return err
	}
	for _, ref := range w.refs {
		if err := w.checkRef(ref); err != nil {
			return err
		}
	}
	return nil
}

// checkRef accepts a reference to an examined subschema or to the anchor of one.
func (w *schemaWalk) checkRef(ref string) error {
	frag := strings.TrimPrefix(ref, "#")
	if frag != "" && !strings.HasPrefix(frag, "/") {
		name, err := url.PathUnescape(frag)
		if _, ok := w.anchors[name]; err != nil || !ok {
			return errors.New("data_schema_ref: the anchor of the reference is not in an examined schema")
		}
		return nil
	}
	ptr, err := url.PathUnescape(frag)
	if err != nil {
		return errors.New("data_schema_ref: the pointer of the reference is not valid")
	}
	if _, ok := w.pointers[ptr]; !ok {
		return errors.New("data_schema_ref: the target of the reference is not a subschema that the service examines")
	}
	return nil
}

// escapePointer escapes one reference token of a JSON Pointer.
func escapePointer(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

func (w *schemaWalk) walk(v any, depth int, ptr string) error {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	if depth > maxSchemaDepth {
		return fmt.Errorf("data_schema_depth: more than %d levels", maxSchemaDepth)
	}
	w.pointers[ptr] = struct{}{}
	if _, has := m["$id"]; has {
		return errors.New("data_schema_id: $id must not be set")
	}
	for _, k := range []string{"$ref", "$dynamicRef"} {
		if s, ok := m[k].(string); ok {
			if !strings.HasPrefix(s, "#") {
				return fmt.Errorf("data_schema_ref: %s must be a local reference that starts with #", k)
			}
			w.refs = append(w.refs, s)
		}
	}
	for _, k := range []string{"$anchor", "$dynamicAnchor"} {
		if s, ok := m[k].(string); ok {
			w.anchors[s] = struct{}{}
		}
	}
	for _, k := range schemaKeywords {
		if err := w.walk(m[k], depth+1, ptr+"/"+k); err != nil {
			return err
		}
	}
	for _, k := range schemaListKeywords {
		if list, ok := m[k].([]any); ok {
			for i, item := range list {
				if err := w.walk(item, depth+1, fmt.Sprintf("%s/%s/%d", ptr, k, i)); err != nil {
					return err
				}
			}
		}
	}
	for _, k := range schemaMapKeywords {
		if sub, ok := m[k].(map[string]any); ok {
			for name, item := range sub {
				if err := w.walk(item, depth+1, ptr+"/"+k+"/"+escapePointer(name)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
