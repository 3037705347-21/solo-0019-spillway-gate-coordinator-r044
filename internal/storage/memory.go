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
) (BackwaterOutcome, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, exists := m.gates[action.GateID]
	if !exists {
		return BackwaterOutcome{}, domain.NotFound("gate_not_found", "gate id was not found")
	}
	reportID := strings.TrimSpace(action.ReportID)
	if reportID == "" {
		return BackwaterOutcome{}, domain.Invalid("invalid_backwater_report", "report id is required")
	}

	key := backwaterKey(action.GateID, reportID)
	if record, exists := m.backwater[key]; exists {
		// The round was already resolved: replay the recorded outcome so
		// repeated reports never spawn conflicting fallback actions.
		return m.replayBackwaterLocked(record, gate, action.Exceeded), nil
	}

	if !action.Exceeded {
		state := backwaterClear
		if m.cancelQueuedBackwaterLocked(action.GateID) {
			state = backwaterCanceled
		}
		m.backwater[key] = backwaterRecord{
			GateID:           action.GateID,
			ReportID:         reportID,
			State:            state,
			FallbackAperture: action.FallbackAperture,
		}
		m.backwaterByGate[action.GateID] = key
		return BackwaterOutcome{State: state, Gate: gate}, nil
	}

	if action.FallbackAperture < 0 || action.FallbackAperture > gate.MaxAperture {
		return BackwaterOutcome{}, domain.Invalid("invalid_fallback_aperture", "fallback aperture is outside the gate limit")
	}

	record := backwaterRecord{
		GateID:           action.GateID,
		ReportID:         reportID,
		CommandID:        action.CommandID,
		Token:            action.Token,
		FallbackAperture: action.FallbackAperture,
	}

	if gate.Status != domain.GateIdle {
		// Another command owns the gate: queue the fallback instead of
		// interrupting the in-flight command. The newest report supersedes
		// any fallback still waiting on this gate.
		m.cancelQueuedBackwaterLocked(action.GateID)
		record.State = backwaterQueued
		m.backwater[key] = record
		m.backwaterByGate[action.GateID] = key
		return BackwaterOutcome{State: backwaterQueued, CommandID: record.CommandID, Gate: gate}, nil
	}

	command, nextGate, err := m.runBackwaterFallbackLocked(record, now)
	if err != nil {
		return BackwaterOutcome{}, err
	}
	record.State = backwaterCompleted
	record.Token = ""
	m.backwater[key] = record
	m.backwaterByGate[action.GateID] = key
	return BackwaterOutcome{
		State:     backwaterCompleted,
		CommandID: command.ID,
		Command:   cloneCommand(command),
		Gate:      nextGate,
	}, nil
}

func (m *MemoryRepository) replayBackwaterLocked(
	record backwaterRecord,
	gate domain.Gate,
	exceeded bool,
) BackwaterOutcome {
	if record.State == backwaterQueued && !exceeded {
		// The exceedance cleared before the queued fallback ran: cancel it
		// and persist the cancellation so it can never fire later.
		record.State = backwaterCanceled
		record.Token = ""
		m.backwater[backwaterKey(record.GateID, record.ReportID)] = record
		return BackwaterOutcome{State: backwaterCanceled, CommandID: record.CommandID, Gate: gate}
	}

	outcome := BackwaterOutcome{
		State:     record.State,
		CommandID: record.CommandID,
		Gate:      gate,
	}
	if record.State == backwaterCompleted && record.CommandID != "" {
		if command, exists := m.commands[record.CommandID]; exists {
			outcome.Command = cloneCommand(command)
		}
	}
	return outcome
}

func (m *MemoryRepository) cancelQueuedBackwaterLocked(gateID string) bool {
	key, exists := m.backwaterByGate[gateID]
	if !exists {
		return false
	}
	record, exists := m.backwater[key]
	if !exists || record.State != backwaterQueued {
		return false
	}
	record.State = backwaterCanceled
	record.Token = ""
	m.backwater[key] = record
	return true
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
	record, exists := m.backwater[key]
	if !exists || record.State != backwaterQueued {
		return gate
	}

	if record.FallbackAperture == gate.Aperture {
		// The gate already sits at the fallback aperture: nothing to move.
		record.State = backwaterCompleted
		record.Token = ""
		m.backwater[key] = record
		return gate
	}

	_, nextGate, err := m.runBackwaterFallbackLocked(record, now)
	if err != nil {
		return gate
	}
	record.State = backwaterCompleted
	record.Token = ""
	m.backwater[key] = record
	return nextGate
}

// runBackwaterFallbackLocked drives one automatic fallback command through
// review, execution, and settlement on an idle gate, storing both the
// command and the resulting gate atomically.
func (m *MemoryRepository) runBackwaterFallbackLocked(
	record backwaterRecord,
	now time.Time,
) (domain.GateCommand, domain.Gate, error) {
	gate := m.gates[record.GateID]
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
		return domain.GateCommand{}, domain.Gate{}, err
	}
	decision, err := domain.NewInterlockDecision(
		"clear",
		"backwater-controller",
		"automatic downstream fallback",
		now,
	)
	if err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	if err := command.Review(decision, record.Token, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	if err := command.BeginExecution(record.Token, "backwater-controller", now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	nextGate := gate
	if err := nextGate.Reserve(command.ID, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	if err := nextGate.StartMovement(command.ID, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	if err := nextGate.Settle(command.TargetAperture, false, now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	if err := command.Complete(now); err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}

	m.commands[command.ID] = command
	m.gates[gate.ID] = nextGate
	return command, nextGate, nil
}

func cloneCommand(command domain.GateCommand) domain.GateCommand {
	if command.Interlock == nil {
		return command
	}
	decision := *command.Interlock
	command.Interlock = &decision
	return command
}
