// Package synapsys connects ordinary Go business functions to Synapsys Core.
//
// The package is framework-neutral. A Worker reports registered processes to
// Core, applies token-gated start and stop commands, and optionally forwards
// process-scoped logs. Synapsys does not schedule functions: an Endless process
// keeps itself alive until it observes cancellation, while a Progressive process
// runs once and returns naturally.
package synapsys
