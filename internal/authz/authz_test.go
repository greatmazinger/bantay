package authz

import (
	"errors"
	"testing"
)

const (
	astrid = "astrid"
	bheng  = "bheng"

	// SPIFFE-shaped identities, for the administrative role.
	adminPrefix   = "spiffe://jobworker.local/admin/"
	clientAlice   = "spiffe://jobworker.local/client/alice"
	clientBob     = "spiffe://jobworker.local/client/bob"
	adminAlice    = "spiffe://jobworker.local/admin/alice"
	notQuiteAdmin = "spiffe://jobworker.local/administrator/mallory"
)

func TestOwnerPolicy_Authorize(t *testing.T) {
	policy := NewPolicy("")

	t.Run("matching actor is authorized", func(t *testing.T) {
		if err := policy.Authorize(astrid, astrid); err != nil {
			t.Errorf("expected nil (authorized), but got %v", err)
		}
	})

	t.Run("wrong actor is forbidden", func(t *testing.T) {
		if err := policy.Authorize(bheng, astrid); !errors.Is(err, ErrForbidden) {
			t.Errorf("expected %v (forbidden), but got %v", ErrForbidden, err)
		}
	})
}

// A job whose owner is the empty string cannot be reached by an actor whose
// identity is also empty. Identity comes from a verified certificate, so an
// empty actor means the server is misconfigured, and it must not match an
// owner field that was never set either.
func TestOwnerPolicy_EmptyIdentities(t *testing.T) {
	policy := NewPolicy("")

	t.Run("empty actor against a real owner is forbidden", func(t *testing.T) {
		if err := policy.Authorize("", astrid); !errors.Is(err, ErrForbidden) {
			t.Errorf("expected %v (forbidden), but got %v", ErrForbidden, err)
		}
	})

	t.Run("real actor against an empty owner is forbidden", func(t *testing.T) {
		if err := policy.Authorize(bheng, ""); !errors.Is(err, ErrForbidden) {
			t.Errorf("expected %v (forbidden), but got %v", ErrForbidden, err)
		}
	})
}

func TestPolicy_AdministrativeRole(t *testing.T) {
	policy := NewPolicy(adminPrefix)

	t.Run("owner is authorized", func(t *testing.T) {
		if err := policy.Authorize(clientAlice, clientAlice); err != nil {
			t.Errorf("owner should be authorized, got %v", err)
		}
	})

	t.Run("administrator may act on a job it does not own", func(t *testing.T) {
		if err := policy.Authorize(adminAlice, clientAlice); err != nil {
			t.Errorf("administrator should be authorized, got %v", err)
		}
	})

	t.Run("non-owner without the role is forbidden", func(t *testing.T) {
		if err := policy.Authorize(clientBob, clientAlice); !errors.Is(err, ErrForbidden) {
			t.Errorf("expected %v, got %v", ErrForbidden, err)
		}
	})

	t.Run("prefix must end at a path separator", func(t *testing.T) {
		// /administrator/ is not /admin/. Without the trailing slash in the
		// prefix this would authorize.
		if err := policy.Authorize(notQuiteAdmin, clientAlice); !errors.Is(err, ErrForbidden) {
			t.Errorf("expected %v, got %v", ErrForbidden, err)
		}
	})
}

func TestPolicy_EmptyAdminPrefixGrantsNothing(t *testing.T) {
	// Every string has "" as a prefix. An unset admin prefix must not make
	// every caller an administrator.
	policy := NewPolicy("")

	if err := policy.Authorize(adminAlice, clientAlice); !errors.Is(err, ErrForbidden) {
		t.Errorf("empty admin prefix should grant nothing, got %v", err)
	}
	if err := policy.Authorize(clientBob, clientAlice); !errors.Is(err, ErrForbidden) {
		t.Errorf("empty admin prefix should grant nothing, got %v", err)
	}
}
