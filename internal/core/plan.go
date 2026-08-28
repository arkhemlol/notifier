package core

import (
	"encoding/hex"
	"hash/fnv"
	"slices"
	"strconv"
)

// PlanID identifies a registered plan, derived from its policy and destinations.
type PlanID string

// Policy determines when a plan is considered complete across its destinations.
type Policy int

// Policy values, from unset to the two completion rules NewPlan accepts.
const (
	PolicyUnknown Policy = iota
	// PolicyAll requires every destination to deliver successfully.
	PolicyAll
	// PolicyFirstSuccess completes after the first successful destination.
	PolicyFirstSuccess
)

// Plan is the immutable destination snapshot a dispatcher delivers against.
// NewDispatcher derives it from the destinations it is given.
type Plan struct {
	id           PlanID
	policy       Policy
	destinations []DestinationID
}

// NewPlan derives a Plan's ID from its policy and destinations.
func NewPlan(policy Policy, destinations []DestinationID) Plan {
	ordered := slices.Clone(destinations)
	if policy == PolicyAll {
		slices.Sort(ordered)
	}

	return Plan{
		id:           derivePlanID(policy, ordered),
		policy:       policy,
		destinations: ordered,
	}
}

// "v1/" leaves room to change the scheme without reusing an old plan's identity.
func derivePlanID(policy Policy, destinations []DestinationID) PlanID {
	digest := fnv.New128a()

	scratch := make([]byte, 0, 24)
	scratch = append(scratch, "v1/"...)
	scratch = strconv.AppendInt(scratch, int64(policy), 10)
	_, _ = digest.Write(scratch)

	for _, destination := range destinations {
		scratch = append(scratch[:0], '/')
		scratch = strconv.AppendInt(scratch, int64(len(destination)), 10)
		scratch = append(scratch, ':')
		scratch = append(scratch, destination...)
		_, _ = digest.Write(scratch)
	}

	sum := digest.Sum(scratch[:0]) // scratch has spare capacity, so this reuses its backing array

	return PlanID("plan-" + hex.EncodeToString(sum))
}

// ID returns the plan's derived identity.
func (p Plan) ID() PlanID {
	return p.id
}

// Policy returns the plan's completion rule.
func (p Plan) Policy() Policy {
	return p.policy
}

// Destinations returns a clone, so no caller can rewrite a registered plan in place.
func (p Plan) Destinations() []DestinationID {
	return slices.Clone(p.destinations)
}
