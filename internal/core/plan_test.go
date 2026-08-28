package core

import (
	"slices"
	"testing"
)

func TestNewPlanDerivesIdentityFromContents(t *testing.T) {
	t.Parallel()

	all := func(destinations ...DestinationID) Plan {
		t.Helper()

		return NewPlan(PolicyAll, destinations)
	}

	base := all("telegram:alerts", "email:on-call")
	if base.Policy() != PolicyAll {
		t.Errorf("Policy() = %v, want PolicyAll", base.Policy())
	}

	if reordered := all("email:on-call", "telegram:alerts"); reordered.ID() != base.ID() {
		t.Errorf("PolicyAll id changed with order: %q vs %q", reordered.ID(), base.ID())
	}

	if added := all("telegram:alerts", "email:on-call", "slack:ops"); added.ID() == base.ID() {
		t.Error("adding a destination did not change the plan id")
	}

	first := NewPlan(PolicyFirstSuccess, []DestinationID{"telegram:alerts", "email:on-call"})
	if first.Policy() != PolicyFirstSuccess {
		t.Errorf("Policy() = %v, want PolicyFirstSuccess", first.Policy())
	}

	reversed := NewPlan(PolicyFirstSuccess, []DestinationID{"email:on-call", "telegram:alerts"})

	if first.ID() == reversed.ID() {
		t.Error("PolicyFirstSuccess ignored destination order")
	}

	want := []DestinationID{"telegram:alerts", "email:on-call"}
	if !slices.Equal(first.Destinations(), want) {
		t.Errorf("Destinations() = %v, want %v", first.Destinations(), want)
	}

	if first.ID() == base.ID() {
		t.Error("policy did not affect the plan id")
	}
}

func TestPlanDestinationsIsACopy(t *testing.T) {
	t.Parallel()

	plan := NewPlan(PolicyAll, []DestinationID{"telegram:alerts", "email:on-call"})

	plan.Destinations()[0] = "attacker:sink"

	if slices.Contains(plan.Destinations(), "attacker:sink") {
		t.Error("Destinations() handed out the plan's own slice")
	}
}
