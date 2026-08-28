package core

import "testing"

func BenchmarkNewPlan(b *testing.B) {
	destinations := []DestinationID{
		"telegram:alerts",
		"email:on-call",
		"email:escalation",
		"slack:ops",
	}

	b.ReportAllocs()

	for b.Loop() {
		if plan := NewPlan(PolicyAll, destinations); plan.ID() == "" {
			b.Fatal("NewPlan() derived an empty plan id")
		}
	}
}
