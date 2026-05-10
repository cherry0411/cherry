// Package filter provides a composable, extensible metadata filter chain for
// the cherry-picker DHT crawler.
//
// Each [Rule] is a pure function that decides whether a piece of metadata should
// be rejected before it is reported to the backend. Rules are applied in order;
// the first rejection terminates the chain ("fail-fast").
//
// Adding a new filter rule requires only implementing a Rule function and
// registering it in the [Chain] — no changes to the calling code are needed.
package filter

import "cherry-picker/internal/pipeline"

// Reason is a short, machine-readable label identifying why metadata was
// rejected. An empty Reason (ReasonPass) means the metadata passed all rules.
type Reason string

const (
	// ReasonPass means no rule rejected the metadata.
	ReasonPass Reason = ""

	// ReasonTooManyFiles: file count exceeds the hard cap.
	ReasonTooManyFiles Reason = "too_many_files"

	// ReasonNonChinese: high file count with no Chinese characters in any path.
	ReasonNonChinese Reason = "non_chinese_files"

	// ReasonNumericFileNames: moderate file count but every filename is purely
	// numeric (digits only, extension stripped).
	ReasonNumericFileNames Reason = "numeric_file_names"
)

// Rule is a single filter predicate. It returns a non-empty [Reason] when the
// metadata should be rejected, or [ReasonPass] to allow it through.
type Rule func(m *pipeline.Metadata) Reason

// namedRule pairs a Rule with its human-readable name for diagnostics.
type namedRule struct {
	name string
	fn   Rule
}

// Chain applies an ordered list of [Rule]s to a piece of metadata.
// Rules are evaluated sequentially; the first rejection terminates the chain.
//
// Chain is not safe for concurrent mutation (Add), but Apply is safe to call
// concurrently once the chain is fully built.
type Chain struct {
	rules []namedRule
}

// NewChain creates an empty Chain. Populate it with [Chain.Add] before use.
func NewChain() *Chain { return &Chain{} }

// Add appends a named rule to the chain. Rules are checked in insertion order.
func (c *Chain) Add(name string, fn Rule) {
	c.rules = append(c.rules, namedRule{name: name, fn: fn})
}

// Apply runs all rules against m and returns the first rejection [Reason], or
// [ReasonPass] if all rules passed. Returns [ReasonPass] when m is nil.
func (c *Chain) Apply(m *pipeline.Metadata) Reason {
	if m == nil {
		return ReasonPass
	}
	for _, r := range c.rules {
		if reason := r.fn(m); reason != ReasonPass {
			return reason
		}
	}
	return ReasonPass
}

// Len returns the number of rules currently in the chain.
func (c *Chain) Len() int { return len(c.rules) }
