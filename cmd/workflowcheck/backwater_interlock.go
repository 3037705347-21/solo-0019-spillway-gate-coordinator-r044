package main

import (
	"fmt"
	"net/http"
)

// backwater-interlock 校验下游越限联动：重复上报幂等、回退身份稳定、
// 在途人工动作不被打断、越限解除后排队回退取消、人工执行凭据保持原样。
func runBackwaterInterlock(client *apiClient) error {
	if _, err := fixtureGate(client, "gate-backwater", "回退联动闸", 5, 1.0); err != nil {
		return err
	}

	// 人工命令先通过复核并占用闸门。
	manual, err := fixtureCommand(client, "gate-backwater", 3.0, "人工调度", "dispatcher-lin")
	if err != nil {
		return err
	}
	manualID, err := stringField(manual, "id")
	if err != nil {
		return err
	}
	approved, err := fixtureClearReview(client, manualID, "safety-wu")
	if err != nil {
		return err
	}
	token, err := stringField(approved, "execution_token")
	if err != nil {
		return err
	}

	// 第一轮越限：闸门被占用，回退排队并获得稳定身份。
	first, err := client.post("/gates/gate-backwater/backwater", map[string]any{
		"report_id":         "incident-1",
		"level":             4.2,
		"limit":             3.0,
		"fallback_aperture": 0.5,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(first, http.StatusOK, "queue first exceeded report"); err != nil {
		return err
	}
	firstState, err := stringField(first.Body, "state")
	if err != nil {
		return err
	}
	if firstState != "queued" {
		return fmt.Errorf("first report state: expected queued, got %q", firstState)
	}
	fallbackID, err := stringField(first.Body, "command_id")
	if err != nil || fallbackID == "" {
		return fmt.Errorf("queued report must reserve a fallback command id: %v", err)
	}

	// 同一轮越限重复上报：仍排队、身份不变，不产生第二份回退。
	for i := 0; i < 3; i++ {
		replay, err := client.post("/gates/gate-backwater/backwater", map[string]any{
			"report_id":         "incident-1",
			"level":             4.4,
			"limit":             3.0,
			"fallback_aperture": 0.5,
		})
		if err != nil {
			return err
		}
		if err := requireStatus(replay, http.StatusOK, "replay exceeded report"); err != nil {
			return err
		}
		state, _ := stringField(replay.Body, "state")
		id, _ := stringField(replay.Body, "command_id")
		if state != "queued" || id != fallbackID {
			return fmt.Errorf("replay %d diverged: state=%q id=%q want queued/%s", i, state, id, fallbackID)
		}
	}

	// 在途人工动作未被打断：闸门仍 reserved 给人工命令。
	gateResult, err := client.get("/gates/gate-backwater")
	if err != nil {
		return err
	}
	gate, err := objectField(gateResult.Body, "gate")
	if err != nil {
		return err
	}
	gateStatus, _ := stringField(gate, "status")
	aperture, _ := floatField(gate, "aperture")
	active, _ := stringField(gate, "active_command_id")
	if gateStatus != "reserved" || aperture != 1.0 || active != manualID {
		return fmt.Errorf("manual reservation disturbed: status=%q aperture=%v active=%q", gateStatus, aperture, active)
	}

	// 越限很快解除：排队回退取消，人工命令照常执行。
	cleared, err := client.post("/gates/gate-backwater/backwater", map[string]any{
		"report_id":         "incident-1",
		"level":             2.0,
		"limit":             3.0,
		"fallback_aperture": 0.5,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(cleared, http.StatusOK, "clear queued report"); err != nil {
		return err
	}
	clearedState, err := stringField(cleared.Body, "state")
	if err != nil {
		return err
	}
	if clearedState != "canceled" {
		return fmt.Errorf("cleared report state: expected canceled, got %q", clearedState)
	}

	executed, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(executed, http.StatusOK, "execute manual command"); err != nil {
		return err
	}
	gate, err = objectField(executed.Body, "gate")
	if err != nil {
		return err
	}
	gateStatus, _ = stringField(gate, "status")
	aperture, _ = floatField(gate, "aperture")
	active, _ = stringField(gate, "active_command_id")
	if gateStatus != "idle" || aperture != 3.0 || active != "" {
		return fmt.Errorf("manual outcome mismatch: status=%q aperture=%v active=%q", gateStatus, aperture, active)
	}

	// 取消的回退从未物化为命令；人工令牌终态后失效。
	fallbackLookup, err := client.get("/commands/" + fallbackID)
	if err != nil {
		return err
	}
	if err := requireStatus(fallbackLookup, http.StatusNotFound, "canceled fallback never materialized"); err != nil {
		return err
	}
	reused, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		return err
	}
	if err := requireStatus(reused, http.StatusConflict, "manual token reuse rejected"); err != nil {
		return err
	}

	fmt.Println("backwater-interlock: duplicates idempotent, manual action intact, queued fallback canceled cleanly")
	return nil
}
