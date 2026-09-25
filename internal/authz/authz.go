// Package authz is the Policy Decision Point (PDP) for the jobmanager. It
// decides, given an actor and a job's owner, whether the actor may act. It
// holds no state and reaches into nothing: resolving a job ID to its owner is
// the caller's job, because the caller has already done that lookup.
package authz

import (
	"errors"
	"strings"
)

// ErrForbidden is returned when an actor may not act on a job.
var ErrForbidden = errors.New("authz: insufficient permissions")

// Policy is the PDP. An actor may act on a job it owns. An actor whose
// identity begins with adminPrefix is an administrator and may act on any job,
// whether or not it started it.
//
// The consuming package declares whatever interface it needs over this.
type Policy struct {
	// adminPrefix, when non-empty, grants any actor whose identity begins with
	// it the right to act on every job.
	adminPrefix string
}

// NewPolicy creates a Policy.
//
// adminPrefix names the administrative role: any actor whose identity begins
// with it may act on any job. Pass an empty string to disable the role. The
// empty case is checked explicitly rather than relying on prefix matching,
// because every string has the empty string as a prefix, so an unset value
// would otherwise make every caller an administrator.
func NewPolicy(adminPrefix string) *Policy {
	return &Policy{adminPrefix: adminPrefix}
}

// Authorize decides whether actor may act on a job owned by owner.
//
// The caller resolves the job first, so a job that does not exist never
// reaches here: that is the registry's answer to give, not the policy's.
func (p *Policy) Authorize(actor string, owner string) error {
	if actor == owner {
		return nil
	}
	if p.adminPrefix != "" && strings.HasPrefix(actor, p.adminPrefix) {
		return nil
	}
	return ErrForbidden
}
