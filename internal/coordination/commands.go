package coordination

import (
	"context"
	"strings"

	"example.com/spillway-gate-coordinator/internal/contracts"
	"example.com/spillway-gate-coordinator/internal/domain"
	"example.com/spillway-gate-coordinator/internal/storage"
)

func (s *Service) RequestCommand(
	ctx context.Context,
	request contracts.RequestCommandRequest,
) (domain.GateCommand, error) {
	gate, err := s.repository.Gate(ctx, request.GateID)
	if err != nil {
		return domain.GateCommand{}, err
	}

	commandID, err := s.newID("cmd")
	if err != nil {
		return domain.GateCommand{}, domain.Internal("command_id_generation_failed", "command id could not be generated")
	}

	command, err := domain.NewGateCommand(
		commandID,
		gate.ID,
		request.TargetAperture,
		gate.Aperture,
		gate.MaxAperture,
		request.Reason,
		request.RequestedBy,
		s.now(),
	)
	if err != nil {
		return domain.GateCommand{}, err
	}
	if err := s.repository.CreateCommand(ctx, command); err != nil {
		return domain.GateCommand{}, err
	}
	return command, nil
}

func (s *Service) Command(ctx context.Context, id string) (domain.GateCommand, error) {
	return s.repository.Command(ctx, id)
}

// BackwaterOutcome is the coordination-level result of a backwater report.
type BackwaterOutcome struct {
	State     string
	CommandID string
	Command   domain.GateCommand
	Gate      domain.Gate
}

func (s *Service) ReportBackwater(
	ctx context.Context,
	gateID string,
	request contracts.ReportBackwaterRequest,
) (BackwaterOutcome, error) {
	reportID := strings.TrimSpace(request.ReportID)
	if reportID == "" {
		return BackwaterOutcome{}, domain.Invalid(
			"invalid_backwater_report",
			"report id is required",
		)
	}

	action := storage.BackwaterAction{
		GateID:           gateID,
		ReportID:         reportID,
		Exceeded:         request.Level > request.Limit,
		FallbackAperture: request.FallbackAperture,
	}

	result, err := s.repository.ReportBackwater(
		ctx,
		action,
		s.now(),
		func() (string, error) {
			return s.newID("bw")
		},
		s.newToken,
	)
	if err != nil {
		return BackwaterOutcome{}, err
	}
	return BackwaterOutcome{
		State:     result.State,
		CommandID: result.CommandID,
		Command:   result.Command,
		Gate:      result.Gate,
	}, nil
}

func (s *Service) ReviewInterlock(
	ctx context.Context,
	commandID string,
	request contracts.ReviewInterlockRequest,
) (domain.GateCommand, domain.Gate, error) {
	decision, err := domain.NewInterlockDecision(
		request.Verdict,
		request.Verifier,
		request.Note,
		s.now(),
	)
	if err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	token := ""
	if decision.IsClear() {
		token, err = s.newToken()
		if err != nil {
			return domain.GateCommand{}, domain.Gate{}, domain.Internal(
				"execution_token_generation_failed",
				"execution token could not be generated",
			)
		}
	}

	return s.repository.ReviewCommand(ctx, commandID, decision, token, s.now())
}

func (s *Service) ExecuteCommand(
	ctx context.Context,
	commandID string,
	request contracts.ExecuteCommandRequest,
) (domain.GateCommand, domain.Gate, error) {
	return s.repository.ExecuteCommand(
		ctx,
		commandID,
		request.Token,
		request.Operator,
		request.SimulateFault,
		s.now(),
	)
}
