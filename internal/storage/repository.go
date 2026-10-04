package storage

import (
	"context"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

type Repository interface {
	CreateGate(ctx context.Context, gate domain.Gate) error
	Gate(ctx context.Context, id string) (domain.Gate, error)
	CreateCommand(ctx context.Context, command domain.GateCommand) error
	Command(ctx context.Context, id string) (domain.GateCommand, error)
	ReportBackwater(
		ctx context.Context,
		action BackwaterAction,
		now time.Time,
		newCommandID func() (string, error),
		newToken func() (string, error),
	) (BackwaterResult, error)
	ReviewCommand(
		ctx context.Context,
		commandID string,
		decision domain.InterlockDecision,
		token string,
		now time.Time,
	) (domain.GateCommand, domain.Gate, error)
	ExecuteCommand(
		ctx context.Context,
		commandID string,
		token string,
		operator string,
		simulateFault bool,
		now time.Time,
	) (domain.GateCommand, domain.Gate, error)
}

type BackwaterAction struct {
	GateID           string
	ReportID         string
	Exceeded         bool
	FallbackAperture float64
}

// BackwaterResult is the single outcome of one backwater round. CommandID is
// the stable fallback identity even while the round is still queued; Command
// is populated only once the fallback movement has materialized.
type BackwaterResult struct {
	State     string
	CommandID string
	Command   domain.GateCommand
	Gate      domain.Gate
}
