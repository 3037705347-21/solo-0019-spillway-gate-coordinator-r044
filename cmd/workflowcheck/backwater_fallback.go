package main

import (
	"fmt"
	"net/http"
)

func runBackwaterFallback(client *apiClient) error {
	if err := checkImmediateFallback(client); err != nil {
		return err
	}
	if err := checkQueuedFallback(client); err != nil {
		return err
	}
	if err := checkCanceledFallback(client); err != nil {
		return err
	}
	fmt.Println("backwater-fallback: immediate, queued, and canceled rounds stay consistent")
	return nil
}

func checkImmediateFallback(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-north", "北侧泄洪闸", 4, 2.5); err != nil {
		return err
	}

	first, err := reportBackwater(client, "gate-north", "round-north-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(first, "completed", "immediate fallback"); err != nil {
		return err
	}
	commandID, err := stringField(first.Body, "command_id")
	if err != nil {
		return err
	}
	gate, err := objectField(first.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(gate, "idle", 1.0, "immediate fallback gate"); err != nil {
		return err
	}

	replay, err := reportBackwater(client, "gate-north", "round-north-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(replay, "completed", "replayed fallback report"); err != nil {
		return err
	}
	replayCommandID, err := stringField(replay.Body, "command_id")
	if err != nil {
		return err
	}
	if replayCommandID != commandID {
		return fmt.Errorf("replayed report spawned a new fallback: %q then %q", commandID, replayCommandID)
	}
	replayGate, err := objectField(replay.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(replayGate, "idle", 1.0, "replayed fallback gate"); err != nil {
		return err
	}
	return nil
}

func checkQueuedFallback(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-south", "南侧泄洪闸", 4, 0.5); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-south", 2.0, "下游补水", "dispatcher-lin")
	if err != nil {
		return err
	}
	commandID, err := stringField(command, "id")
	if err != nil {
		return err
	}
	approved, err := fixtureClearReview(client, commandID, "safety-wu")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	queued, err := reportBackwater(client, "gate-south", "round-south-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(queued, "queued", "fallback behind in-flight command"); err != nil {
		return err
	}
	fallbackID, err := stringField(queued.Body, "command_id")
	if err != nil {
		return err
	}
	queuedGate, err := objectField(queued.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(queuedGate, "reserved", 0.5, "reserved gate untouched by queued fallback"); err != nil {
		return err
	}

	executed, err := client.post("/commands/"+commandID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(executed, http.StatusOK, "execute in-flight command despite queued fallback"); err != nil {
		return err
	}
	executedCommand, err := objectField(executed.Body, "command")
	if err != nil {
		return err
	}
	if err := requireCommandStatus(executedCommand, "completed", "in-flight command"); err != nil {
		return err
	}
	settledGate, err := objectField(executed.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(settledGate, "idle", 1.0, "gate settled at fallback aperture"); err != nil {
		return err
	}

	fallback, err := client.get("/commands/" + fallbackID)
	if err != nil {
		return err
	}
	if err := requireStatus(fallback, http.StatusOK, "read fired fallback command"); err != nil {
		return err
	}
	fallbackCommand, err := objectField(fallback.Body, "command")
	if err != nil {
		return err
	}
	if err := requireCommandStatus(fallbackCommand, "completed", "fired fallback command"); err != nil {
		return err
	}

	replay, err := reportBackwater(client, "gate-south", "round-south-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(replay, "completed", "replayed round after fallback fired"); err != nil {
		return err
	}
	replayCommandID, err := stringField(replay.Body, "command_id")
	if err != nil {
		return err
	}
	if replayCommandID != fallbackID {
		return fmt.Errorf("replayed round changed fallback identity: %q then %q", fallbackID, replayCommandID)
	}
	return nil
}

func checkCanceledFallback(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-east", "东侧泄洪闸", 4, 0.5); err != nil {
		return err
	}
	command, err := fixtureCommand(client, "gate-east", 2.0, "上游水位调整", "dispatcher-lin")
	if err != nil {
		return err
	}
	commandID, err := stringField(command, "id")
	if err != nil {
		return err
	}
	approved, err := fixtureClearReview(client, commandID, "safety-wu")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	queued, err := reportBackwater(client, "gate-east", "round-east-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(queued, "queued", "fallback queued behind in-flight command"); err != nil {
		return err
	}

	cleared, err := reportBackwater(client, "gate-east", "round-east-1", 1, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(cleared, "canceled", "cleared round cancels queued fallback"); err != nil {
		return err
	}

	executed, err := client.post("/commands/"+commandID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(executed, http.StatusOK, "execute command after fallback canceled"); err != nil {
		return err
	}
	settledGate, err := objectField(executed.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(settledGate, "idle", 2.0, "gate settled at human target without fallback"); err != nil {
		return err
	}

	replay, err := reportBackwater(client, "gate-east", "round-east-1", 5, 3, 1.0)
	if err != nil {
		return err
	}
	if err := requireState(replay, "canceled", "canceled round stays canceled"); err != nil {
		return err
	}
	replayGate, err := objectField(replay.Body, "gate")
	if err != nil {
		return err
	}
	if err := requireGate(replayGate, "idle", 2.0, "canceled round never moves the gate"); err != nil {
		return err
	}
	return nil
}

func reportBackwater(
	client *apiClient,
	gateID string,
	reportID string,
	level float64,
	limit float64,
	fallbackAperture float64,
) (apiResult, error) {
	result, err := client.post("/gates/"+gateID+"/backwater", map[string]any{
		"report_id":         reportID,
		"level":             level,
		"limit":             limit,
		"fallback_aperture": fallbackAperture,
	})
	if err != nil {
		return apiResult{}, err
	}
	if err := requireStatus(result, http.StatusOK, "report backwater "+reportID); err != nil {
		return apiResult{}, err
	}
	return result, nil
}

func requireState(result apiResult, expected string, operation string) error {
	state, err := stringField(result.Body, "state")
	if err != nil {
		return err
	}
	if state != expected {
		return fmt.Errorf("%s: expected state %q, got %q", operation, expected, state)
	}
	return nil
}

func requireGate(gate map[string]any, status string, aperture float64, operation string) error {
	gateStatus, err := stringField(gate, "status")
	if err != nil {
		return err
	}
	gateAperture, err := floatField(gate, "aperture")
	if err != nil {
		return err
	}
	if gateStatus != status || gateAperture != aperture {
		return fmt.Errorf(
			"%s: expected status=%q aperture=%v, got status=%q aperture=%v",
			operation,
			status,
			aperture,
			gateStatus,
			gateAperture,
		)
	}
	return nil
}

func requireCommandStatus(command map[string]any, expected string, operation string) error {
	status, err := stringField(command, "status")
	if err != nil {
		return err
	}
	if status != expected {
		return fmt.Errorf("%s: expected command status %q, got %q", operation, expected, status)
	}
	return nil
}
