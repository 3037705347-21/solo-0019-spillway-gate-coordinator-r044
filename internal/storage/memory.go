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
	queuedBackwater map[string][]string
}

type backwaterRecord struct {
	GateID           string
	ReportID         string
	State            string
	CommandID        string
	Token            string
	FallbackAperture float64
}

// Backwater round states. A round is the lifecycle of one report id on one
// gate: it is either completed immediately, queued behind the gate's active
// manual command, canceled by a clearing report, or recorded as clear when no
// fallback was ever required.
const (
	BackwaterQueued    = "queued"
	BackwaterCompleted = "completed"
	BackwaterCanceled  = "canceled"
	BackwaterClear     = "clear"
)

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		gates:           make(map[string]domain.Gate),
		commands:        make(map[string]domain.GateCommand),
		backwater:       make(map[string]backwaterRecord),
		queuedBackwater: make(map[string][]string),
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
	newCommandID func() (string, error),
	newToken func() (string, error),
) (BackwaterResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gate, exists := m.gates[action.GateID]
	if !exists {
		return BackwaterResult{}, domain.NotFound("gate_not_found", "gate id was not found")
	}
	reportID := strings.TrimSpace(action.ReportID)
	if reportID == "" {
		return BackwaterResult{}, domain.Invalid("invalid_backwater_report", "report id is required")
	}
	if action.FallbackAperture < 0 || action.FallbackAperture > gate.MaxAperture {
		return BackwaterResult{}, domain.Invalid(
			"invalid_fallback_aperture",
			"fallback aperture must be within the gate limit",
		)
	}

	key := backwaterKey(action.GateID, reportID)

	// A replay of the same report is the same round: return its recorded
	// outcome instead of creating a second fallback command.
	if record, exists := m.backwater[key]; exists {
		// A later, non-exceeded delivery for the same round releases a
		// fallback that has not started moving yet.
		if !action.Exceeded && record.State == BackwaterQueued {
			record = m.cancelQueuedBackwaterLocked(record)
		}
		return m.backwaterResultLocked(record), nil
	}

	// First delivery for this round. A non-exceeded report requires no
	// fallback at all; the round is recorded as clear and is idempotent.
	if !action.Exceeded {
		record := backwaterRecord{
			GateID:           action.GateID,
			ReportID:         reportID,
			State:            BackwaterClear,
			FallbackAperture: action.FallbackAperture,
		}
		m.backwater[key] = record
		return m.backwaterResultLocked(record), nil
	}

	// Exceeded round: the fallback identity is generated once and survives
	// even while the movement waits behind the gate's active command.
	commandID, err := newCommandID()
	if err != nil {
		return BackwaterResult{}, domain.Internal(
			"backwater_command_id_generation_failed",
			"automatic fallback command id could not be generated",
		)
	}
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return BackwaterResult{}, domain.Internal(
			"backwater_command_id_generation_failed",
			"automatic fallback command id could not be generated",
		)
	}
	token, err := newToken()
	if err != nil {
		return BackwaterResult{}, domain.Internal(
			"backwater_token_generation_failed",
			"automatic fallback token could not be generated",
		)
	}

	record := backwaterRecord{
		GateID:           action.GateID,
		ReportID:         reportID,
		CommandID:        commandID,
		Token:            token,
		FallbackAperture: action.FallbackAperture,
	}

	// The gate is occupied by an active manual command. The fallback must
	// not touch the gate or the operator's command: it waits until the gate
	// settles. A faulted gate is excluded from automatic recovery as well.
	if gate.Status != domain.GateIdle {
		record.State = BackwaterQueued
		m.backwater[key] = record
		m.queuedBackwater[action.GateID] = append(m.queuedBackwater[action.GateID], key)
		return m.backwaterResultLocked(record), nil
	}

	// Gate is free: the automatic fallback runs to completion immediately.
	// If the aperture is already at the fallback value there is nothing to
	// move, but the round is still recorded with one stable identity.
	nextGate := gate
	if gate.Aperture != action.FallbackAperture {
		var command domain.GateCommand
		nextGate, command, err = m.buildFallbackMovementLocked(gate, record, now)
		if err != nil {
			return BackwaterResult{}, err
		}
		record.State = BackwaterCompleted
		m.commands[command.ID] = command
		m.gates[action.GateID] = nextGate
		m.backwater[key] = record
		return BackwaterResult{
			State:     BackwaterCompleted,
			CommandID: command.ID,
			Command:   cloneCommand(command),
			Gate:      cloneGate(nextGate),
		}, nil
	}

	record.State = BackwaterCompleted
	m.backwater[key] = record
	return m.backwaterResultLocked(record), nil
}

// buildFallbackMovementLocked builds one automatic fallback as a normal
// command lifecycle (approved -> executing -> completed) while the gate
// follows idle -> reserved -> moving -> idle. It mutates only the passed
// values; the caller commits the returned command and gate together with the
// round state while still holding m.mu.
func (m *MemoryRepository) buildFallbackMovementLocked(
	gate domain.Gate,
	record backwaterRecord,
	now time.Time,
) (domain.Gate, domain.GateCommand, error) {
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
		return domain.Gate{}, domain.GateCommand{}, err
	}
	decision, err := domain.NewInterlockDecision(
		"clear",
		"backwater-controller",
		"automatic downstream fallback",
		now,
	)
	if err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}
	if err := command.Review(decision, record.Token, now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}
	if err := command.BeginExecution(record.Token, "backwater-controller", now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}

	nextGate := gate
	if err := nextGate.Reserve(command.ID, now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}
	if err := nextGate.StartMovement(command.ID, now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}
	if err := nextGate.Settle(command.TargetAperture, false, now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}
	if err := command.Complete(now); err != nil {
		return domain.Gate{}, domain.GateCommand{}, err
	}

	return nextGate, command, nil
}

// backwaterResultLocked projects a stored round. Only completed rounds expose
// the materialized command; queued and canceled rounds expose the stable
// command identity alone so later reviews and handovers can correlate them.
func (m *MemoryRepository) backwaterResultLocked(record backwaterRecord) BackwaterResult {
	gate := m.gates[record.GateID]
	result := BackwaterResult{
		State:     record.State,
		CommandID: record.CommandID,
		Gate:      cloneGate(gate),
	}
	if record.State == BackwaterCompleted && record.CommandID != "" {
		if command, exists := m.commands[record.CommandID]; exists {
			result.Command = cloneCommand(command)
		}
	}
	return result
}

// cancelQueuedBackwaterLocked releases a fallback that is still waiting behind
// the active manual command. The queued command has not touched the gate, so
// revoking its token and marking the round canceled leaves the manual
// workflow untouched.
func (m *MemoryRepository) cancelQueuedBackwaterLocked(record backwaterRecord) backwaterRecord {
	queue := m.queuedBackwater[record.GateID]
	kept := queue[:0]
	for _, key := range queue {
		if key != backwaterKey(record.GateID, record.ReportID) {
			kept = append(kept, key)
		}
	}
	if len(kept) == 0 {
		delete(m.queuedBackwater, record.GateID)
	} else {
		m.queuedBackwater[record.GateID] = kept
	}
	record.State = BackwaterCanceled
	m.backwater[backwaterKey(record.GateID, record.ReportID)] = record
	return record
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

	// Commit the manual outcome and any queued fallback activations as one
	// unit: activateBackwaterLocked only mutates repository state once every
	// queued movement succeeds, so a failure leaves no partial result behind.
	activatedGate, err := m.activateBackwaterLocked(nextGate, now)
	if err != nil {
		return domain.GateCommand{}, domain.Gate{}, err
	}
	m.commands[nextCommand.ID] = nextCommand
	m.gates[activatedGate.ID] = activatedGate
	return cloneCommand(nextCommand), cloneGate(activatedGate), nil
}

// activateBackwaterLocked drains the queued fallback rounds for a gate that
// has just settled back to idle. Each round becomes a normal, fully traceable
// fallback command; the gate ends idle and unreserved so the next queued
// round can follow. Faulted gates are never recovered automatically. Map
// mutations happen only after every queued movement has succeeded, which
// keeps the manual execution and the fallback activation atomic.
func (m *MemoryRepository) activateBackwaterLocked(gate domain.Gate, now time.Time) (domain.Gate, error) {
	if gate.Status != domain.GateIdle {
		return gate, nil
	}

	keys := append([]string(nil), m.queuedBackwater[gate.ID]...)
	current := gate
	completed := make(map[string]backwaterRecord)
	dequeued := make(map[string]struct{})
	newCommands := make(map[string]domain.GateCommand)

	for _, key := range keys {
		record, exists := m.backwater[key]
		if !exists || record.State != BackwaterQueued || record.CommandID == "" {
			dequeued[key] = struct{}{}
			continue
		}

		// A round may have already reached the fallback aperture (an
		// earlier queued round moved to the same value): close it without
		// creating another movement.
		if current.Aperture == record.FallbackAperture {
			record.State = BackwaterCompleted
			completed[key] = record
			dequeued[key] = struct{}{}
			continue
		}

		next, command, err := m.buildFallbackMovementLocked(current, record, now)
		if err != nil {
			return domain.Gate{}, err
		}
		current = next
		newCommands[command.ID] = command
		record.State = BackwaterCompleted
		completed[key] = record
		dequeued[key] = struct{}{}
	}

	for id, command := range newCommands {
		m.commands[id] = command
	}
	for key, record := range completed {
		m.backwater[key] = record
	}
	for key := range dequeued {
		m.dequeueBackwaterLocked(gate.ID, key)
	}
	return current, nil
}

func (m *MemoryRepository) dequeueBackwaterLocked(gateID, key string) {
	queue := m.queuedBackwater[gateID]
	kept := queue[:0]
	for _, queuedKey := range queue {
		if queuedKey != key {
			kept = append(kept, queuedKey)
		}
	}
	if len(kept) == 0 {
		delete(m.queuedBackwater, gateID)
	} else {
		m.queuedBackwater[gateID] = kept
	}
}

func cloneGate(gate domain.Gate) domain.Gate {
	return gate
}

func cloneCommand(command domain.GateCommand) domain.GateCommand {
	if command.Interlock == nil {
		return command
	}
	decision := *command.Interlock
	command.Interlock = &decision
	return command
}
