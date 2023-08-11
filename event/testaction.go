package event

import (
	"context"
)

// IntAction is an action that does nothing but is used to test the scheduler.
// An IntAction is equal to another IntAction if they have the same integer
type IntAction int

var _ Action = (*IntAction)(nil)

// Run does nothing
func (a IntAction) Run(context.Context) {}

// FuncAction is an action that does nothing but tracks whether it was "run"
// yet. It is used to test the scheduler.
type FuncAction struct {
	Ran bool
	Int int
}

var _ Action = (*FuncAction)(nil)

// NewFuncAction returns a new FuncAction
func NewFuncAction(i int) *FuncAction {
	return &FuncAction{Int: i}
}

// Run sets Ran to true
func (a *FuncAction) Run(context.Context) {
	a.Ran = true
}
