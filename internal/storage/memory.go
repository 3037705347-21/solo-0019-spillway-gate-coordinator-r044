package storage

import (
	"context"
	"strings"
	"sync"
	"time"

	"example.com/spillway-gate-coordinator/internal/domain"
)

type MemoryRepository struct {
	mu              sync.RWMutex
	gates           map[string]domain.Gate
	commands        map[string]domain.GateCommand
	backwater       map[string]backwaterRecord
	backwaterByGate map[string]string
}

type backwaterRecord struct {
	GateID           string
	ReportID         string
	State            string
	CommandID        string
	Token            string
	FallbackAperture float64
}

const (
	backwaterQueued    = "queued"
	backwaterCompleted = "completed"
	backwaterCanceled  = "canceled"
	backwaterClear     = "clear"
)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		gates:           make(map[string]domain.Gate),
		commands:        make(map[string]domain.GateCommand),
		backwater:       make(map[string]backwaterRecord),
		backwaterByGate: make(map[string]string),
	}
}

func backwaterKey(gateID, reportID string) string {
	return gateID + "\x00" + reportID
}

func (m *MemoryRepository) CreateGate(_ context.Context, gate domain.Gate) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.gates[gate.ID]; exists {
		return domain.Conflict("duplicate_gate_id", "gate id already exists")
	}
	m.gates[gate.ID] = gate
	return nil
}

func (m *MemoryRepository) Gate(_ context.Context, id string) (domain.Gate, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	gate, exists := m.gates[id]
	if !exists {
		return domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}
	return gate, nil
}

func (m *MemoryRepository) CreateCommand(_ context.Context, command domain.GateCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.NotFound("gate_not_found", "gate id was not found")
	}
	if gate.Status == domain.GateFaulted {
		return domain.Conflict("gate_faulted", "faulted gates cannot receive new commands")
	}
	if command.StartingAperture != gate.Aperture {
		return domain.Conflict("gate_aperture_changed", "gate aperture changed before the command was stored")
	}
	if command.TargetAperture > gate.MaxAperture {
		return domain.Invalid("invalid_target_aperture", "target aperture is outside the gate limit")
	}
	if _, exists := m.commands[command.ID]; exists {
		return domain.Conflict("duplicate_command_id", "command id already exists")
	}

	m.commands[command.ID] = command
	return nil
}

func (m *MemoryRepository) Command(_ context.Context, id string) (domain.GateCommand, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	command, exists := m.commands[id]
	if !exists {
		return domain.GateCommand{}, domain.NotFound("command_not_found", "command id was not found")
	}
	return cloneCommand(command), nil
}

func (m *MemoryRepository) ReportBackwater(
	_ context.Context,
	action BackwaterAction,
	now time.Time,
) (string, domain.GateCommand, domain.Gate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, exists := m.gates[action.GateID]
	if !exists {
		return "", domain.GateCommand{}, domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}
	reportID := strings.TrimSpace(action.ReportID)
	if reportID == "" {
		return "", domain.GateCommand{}, domain.Gate{}, domain.Invalid("invalid_backwater_report", "report id is required")
	}

	key := backwaterKey(action.GateID, reportID)
	record, exists := m.backwater[key]
	if exists && record.State == backwaterQueued {
		command := domain.GateCommand{}
		if record.CommandID != "" {
			command = cloneCommand(m.commands[record.CommandID])
		}
		return record.State, command, gate, nil
	}

	if !action.Exceeded {
		if !exists {
			record = backwaterRecord{
				GateID:   action.GateID,
				ReportID: reportID,
			}
		}
		if record.State == backwaterQueued {
			return backwaterCanceled, domain.GateCommand{}, gate, nil
		}
		record.State = backwaterClear
		record.FallbackAperture = action.FallbackAperture
		m.backwater[key] = record
		m.backwaterByGate[action.GateID] = key
		return backwaterClear, domain.GateCommand{}, gate, nil
	}

	command, err := domain.NewGateCommand(
		action.CommandID,
		action.GateID,
		action.FallbackAperture,
		gate.Aperture,
		gate.MaxAperture,
		"downstream_backwater_fallback",
		"backwater-controller",
		now,
	)
	if err != nil {
		return "", domain.GateCommand{}, domain.Gate{}, err
	}
	decision, err := domain.NewInterlockDecision(
		"clear",
		"backwater-controller",
		"automatic downstream fallback",
		now,
	)
	if err != nil {
		return "", domain.GateCommand{}, domain.Gate{}, err
	}
	if err := command.Review(decision, action.Token, now); err != nil {
		return "", domain.GateCommand{}, domain.Gate{}, err
	}

	nextGate := gate
	if gate.Status == domain.GateIdle {
		if err := command.BeginExecution(action.Token, "backwater-controller", now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextGate.Reserve(command.ID, now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextGate.StartMovement(command.ID, now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextGate.Settle(command.TargetAperture, false, now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
		if err := command.Complete(now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
	} else {
		if err := command.BeginExecution(action.Token, "backwater-controller", now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
		nextGate.ActiveCommandID = command.ID
		nextGate.Status = domain.GateMoving
		nextGate.Aperture = command.TargetAperture
		nextGate.UpdatedAt = now.UTC()
		nextGate.Version++
		if err := command.Complete(now); err != nil {
			return "", domain.GateCommand{}, domain.Gate{}, err
		}
	}

	m.commands[command.ID] = command
	m.gates[action.GateID] = nextGate
	record = backwaterRecord{
		GateID:           action.GateID,
		ReportID:         reportID,
		State:            backwaterCompleted,
		CommandID:        command.ID,
		FallbackAperture: action.FallbackAperture,
	}
	m.backwater[key] = record
	m.backwaterByGate[action.GateID] = key
	return backwaterCompleted, cloneCommand(command), nextGate, nil
}

func (m *MemoryRepository) ReviewCommand(
	_ context.Context,
	commandID string,
	decision domain.InterlockDecision,
	token string,
	now time.Time,
) (domain.GateCommand, domain.Gate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	command, exists := m.commands[commandID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("command_not_found", "command id was not found")
	}
	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}

	nextCommand := cloneCommand(command)
	if err := nextCommand.Review(decision, token, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	nextGate := gate
	if decision.IsClear() {
		if err := nextGate.Reserve(nextCommand.ID, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	}

	m.commands[nextCommand.ID] = nextCommand
	m.gates[nextGate.ID] = nextGate
	return cloneCommand(nextCommand), nextGate, nil
}

func (m *MemoryRepository) ExecuteCommand(
	_ context.Context,
	commandID string,
	token string,
	operator string,
	simulateFault bool,
	now time.Time,
) (domain.GateCommand, domain.Gate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	command, exists := m.commands[commandID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("command_not_found", "command id was not found")
	}
	gate, exists := m.gates[command.GateID]
	if !exists {
		return domain.GateCommand{}, domain.Gate{}, domain.NotFound("gate_not_found", "gate id was not found")
	}

	nextCommand := cloneCommand(command)
	if err := nextCommand.BeginExecution(token, operator, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	nextGate := gate
	if err := nextGate.StartMovement(nextCommand.ID, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	if simulateFault {
		if err := nextGate.Settle(nextCommand.TargetAperture, true, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextCommand.Abort("simulated_gate_fault", now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	} else {
		if err := nextGate.Settle(nextCommand.TargetAperture, false, now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
		if err := nextCommand.Complete(now); err != nil {
			return domain.GateCommand{}, domain.Gate{}, err
		}
	}

	m.commands[nextCommand.ID] = nextCommand
	m.gates[nextGate.ID] = nextGate
	nextGate = m.activateBackwaterLocked(nextGate, now)
	m.gates[nextGate.ID] = nextGate
	return cloneCommand(nextCommand), nextGate, nil
}

func (m *MemoryRepository) activateBackwaterLocked(gate domain.Gate, now time.Time) domain.Gate {
	if gate.Status != domain.GateIdle {
		return gate
	}
	key, exists := m.backwaterByGate[gate.ID]
	if !exists {
		return gate
	}
	record := m.backwater[key]
	if record.State != backwaterQueued || record.CommandID == "" {
		return gate
	}

	command, err := domain.NewGateCommand(
		record.CommandID,
		gate.ID,
		record.FallbackAperture,
		gate.Aperture,
		gate.MaxAperture,
		"downstream_backwater_fallback",
		"backwater-controller",
		now,
	)
	if err != nil {
		return gate
	}
	decision, err := domain.NewInterlockDecision(
		"clear",
		"backwater-controller",
		"automatic downstream fallback",
		now,
	)
	if err != nil {
		return gate
	}
	if err := command.Review(decision, record.Token, now); err != nil {
		return gate
	}

	nextGate := gate
	if err := command.BeginExecution(record.Token, "backwater-controller", now); err != nil {
		return gate
	}
	if err := nextGate.Reserve(command.ID, now); err != nil {
		return gate
	}
	if err := nextGate.StartMovement(command.ID, now); err != nil {
		return gate
	}
	if err := nextGate.Settle(command.TargetAperture, false, now); err != nil {
		return gate
	}
	if err := command.Complete(now); err != nil {
		return gate
	}

	m.commands[command.ID] = command
	record.State = backwaterCompleted
	record.CommandID = command.ID
	m.backwater[key] = record
	m.gates[gate.ID] = nextGate
	return nextGate
}

func cloneCommand(command domain.GateCommand) domain.GateCommand {
	if command.Interlock == nil {
		return command
	}
	decision := *command.Interlock
	command.Interlock = &decision
	return command
}
