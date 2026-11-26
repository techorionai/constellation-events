// Package errors holds the application errors that a handler shows as a
// Problem of the contract.
package errors

import (
	stderrors "errors"
	"net/http"
	"time"
)

// FieldError is one incorrect field of a request. Field is a JSON Pointer.
type FieldError struct {
	Field   string
	Code    string
	Message string
}

// ValidationError is a 422 error. Code is the code of the Problem. Fields can
// be empty.
type ValidationError struct {
	Code   string
	Fields []FieldError
}

// Error returns the code.
func (e *ValidationError) Error() string { return "validation: " + e.Code }

// AsValidationError returns the ValidationError in the chain.
func AsValidationError(err error) (*ValidationError, bool) {
	var ve *ValidationError
	if stderrors.As(err, &ve) {
		return ve, true
	}
	return nil, false
}

// ProblemError is an error that a handler shows as a Problem with a status and
// a code of the contract, for example 404 event_type_not_found.
type ProblemError struct {
	Status int
	Code   string
	Detail string
	// RetryAfter is the wait time for the header Retry-After. It is 0 if the
	// error has no header.
	RetryAfter time.Duration
}

// Error returns the code and the detail.
func (e *ProblemError) Error() string { return e.Code + ": " + e.Detail }

// NewProblem makes a ProblemError.
func NewProblem(status int, code, detail string) *ProblemError {
	return &ProblemError{Status: status, Code: code, Detail: detail}
}

// DefaultUnavailableRetryAfter is the Retry-After value of a 503 answer.
const DefaultUnavailableRetryAfter = 5 * time.Second

// NewUnavailable makes the 503 ProblemError.
func NewUnavailable() *ProblemError {
	p := NewProblem(http.StatusServiceUnavailable, "unavailable", "The service is not available.")
	p.RetryAfter = DefaultUnavailableRetryAfter
	return p
}

// AsProblem returns the ProblemError in the chain.
func AsProblem(err error) (*ProblemError, bool) {
	var pe *ProblemError
	if stderrors.As(err, &pe) {
		return pe, true
	}
	return nil, false
}

// NewNotFound makes a 404 ProblemError with the code.
func NewNotFound(code, detail string) *ProblemError {
	return NewProblem(http.StatusNotFound, code, detail)
}
