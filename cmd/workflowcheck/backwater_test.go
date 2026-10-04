package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"example.com/spillway-gate-coordinator/internal/bootstrap"
)

func newTestClient(t *testing.T) *apiClient {
	t.Helper()
	server := httptest.NewServer(bootstrap.NewHandler())
	t.Cleanup(server.Close)
	return newAPIClient(server.URL)
}

func mustCommand(t *testing.T, client *apiClient, gateID string, target float64, reason, requestedBy string) map[string]any {
	t.Helper()
	command, err := fixtureCommand(client, gateID, target, reason, requestedBy)
	if err != nil {
		t.Fatalf("create command: %v", err)
	}
	return command
}

func mustReview(t *testing.T, client *apiClient, commandID, verifier string) map[string]any {
	t.Helper()
	command, err := fixtureClearReview(client, commandID, verifier)
	if err != nil {
		t.Fatalf("clear review: %v", err)
	}
	return command
}

func reportBackwater(client *apiClient, gateID, reportID string, level, limit, fallback float64) apiResult {
	result, err := client.post("/gates/"+gateID+"/backwater", map[string]any{
		"report_id":         reportID,
		"level":             level,
		"limit":             limit,
		"fallback_aperture": fallback,
	})
	if err != nil {
		panic(err)
	}
	return result
}

func resultFor(t *testing.T, client *apiClient, path string, payload map[string]any) apiResult {
	t.Helper()
	result, err := client.post(path, payload)
	if err != nil {
		t.Fatalf("call %s: %v", path, err)
	}
	return result
}

func gateState(t *testing.T, client *apiClient, gateID string) (string, float64, string) {
	t.Helper()
	result, err := client.get("/gates/" + gateID)
	if err != nil {
		t.Fatalf("read gate: %v", err)
	}
	if err := requireStatus(result, http.StatusOK, "read gate"); err != nil {
		t.Fatal(err)
	}
	gate, err := objectField(result.Body, "gate")
	if err != nil {
		t.Fatal(err)
	}
	status, _ := stringField(gate, "status")
	aperture, _ := floatField(gate, "aperture")
	active, _ := stringField(gate, "active_command_id")
	return status, aperture, active
}

func commandStatus(t *testing.T, client *apiClient, commandID string) (string, bool) {
	t.Helper()
	result, err := client.get("/commands/" + commandID)
	if err != nil {
		t.Fatalf("read command: %v", err)
	}
	if result.Status == http.StatusNotFound {
		return "", false
	}
	if err := requireStatus(result, http.StatusOK, "read command"); err != nil {
		t.Fatal(err)
	}
	command, err := objectField(result.Body, "command")
	if err != nil {
		t.Fatal(err)
	}
	status, err := stringField(command, "status")
	if err != nil {
		t.Fatal(err)
	}
	return status, true
}

// 空闲闸门收到越限上报：回退立即完成，命令与闸门状态互相解释。
func TestBackwaterImmediateFallback(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-1", "回退闸一", 5, 2.0)

	result := reportBackwater(client, "gate-bw-1", "rpt-1", 4.0, 3.0, 1.0)
	if err := requireStatus(result, http.StatusOK, "report exceeded"); err != nil {
		t.Fatal(err)
	}
	state, _ := stringField(result.Body, "state")
	if state != "completed" {
		t.Fatalf("state: expected completed, got %q", state)
	}
	commandID, err := stringField(result.Body, "command_id")
	if err != nil || commandID == "" {
		t.Fatalf("missing fallback command id: %v body=%s", err, prettyBody(result.Body))
	}
	command, err := objectField(result.Body, "command")
	if err != nil {
		t.Fatalf("fallback response must carry the command: %v", err)
	}
	if status, _ := stringField(command, "status"); status != "completed" {
		t.Fatalf("fallback command status: expected completed, got %q", status)
	}
	if executedBy, _ := stringField(command, "executed_by"); executedBy != "backwater-controller" {
		t.Fatalf("fallback executor: expected backwater-controller, got %q", executedBy)
	}
	interlock, err := objectField(command, "interlock")
	if err != nil {
		t.Fatalf("fallback command missing interlock: %v", err)
	}
	if verifier, _ := stringField(interlock, "verifier"); verifier != "backwater-controller" {
		t.Fatalf("fallback interlock verifier: got %q", verifier)
	}

	status, aperture, active := gateState(t, client, "gate-bw-1")
	if status != "idle" || aperture != 1.0 || active != "" {
		t.Fatalf("gate after fallback: status=%q aperture=%v active=%q", status, aperture, active)
	}

	if got, ok := commandStatus(t, client, commandID); !ok || got != "completed" {
		t.Fatalf("fallback command readback: status=%q found=%v", got, ok)
	}
}

// 同一轮越限重复上报：只产生一份回退动作，响应完全一致。
func TestBackwaterDuplicateReportIsIdempotent(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-2", "回退闸二", 5, 2.0)

	first := reportBackwater(client, "gate-bw-2", "dup-rpt", 4.0, 3.0, 0.5)
	firstID, _ := stringField(first.Body, "command_id")
	for i := 0; i < 5; i++ {
		replay := reportBackwater(client, "gate-bw-2", "dup-rpt", 4.1, 3.0, 0.5)
		if replay.Status != http.StatusOK {
			t.Fatalf("replay %d status %d", i, replay.Status)
		}
		state, _ := stringField(replay.Body, "state")
		id, _ := stringField(replay.Body, "command_id")
		if state != "completed" || id != firstID {
			t.Fatalf("replay %d diverged: state=%q id=%q first=%q", i, state, id, firstID)
		}
	}

	status, aperture, active := gateState(t, client, "gate-bw-2")
	if status != "idle" || aperture != 0.5 || active != "" {
		t.Fatalf("gate after replays: status=%q aperture=%v active=%q", status, aperture, active)
	}
}

// 未越限上报幂等且不会产生任何命令。
func TestBackwaterClearReport(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-3", "回退闸三", 5, 2.0)
	for i := 0; i < 2; i++ {
		result := reportBackwater(client, "gate-bw-3", "clear-rpt", 2.0, 3.0, 1.0)
		if err := requireStatus(result, http.StatusOK, "clear report"); err != nil {
			t.Fatal(err)
		}
		state, _ := stringField(result.Body, "state")
		if state != "clear" {
			t.Fatalf("state: expected clear, got %q", state)
		}
		if _, exists := result.Body["command_id"]; exists {
			t.Fatalf("clear report must not allocate a command identity")
		}
	}
	status, aperture, active := gateState(t, client, "gate-bw-3")
	if status != "idle" || aperture != 2.0 || active != "" {
		t.Fatalf("gate changed after clear reports: status=%q aperture=%v active=%q", status, aperture, active)
	}
}

// 越限发生在人工命令已占用闸门（已复核待执行）时：回退排队等待，
// 不打断在途动作；身份稳定；人工令牌保持有效。
func TestBackwaterQueuedBehindManualCommand(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-4", "回退闸四", 5, 1.0)
	manual := mustCommand(t, client, "gate-bw-4", 3.0, "人工调度", "dispatcher-lin")
	manualID, _ := stringField(manual, "id")
	approved := mustReview(t, client, manualID, "safety-wu")
	token, _ := stringField(approved, "execution_token")

	first := reportBackwater(client, "gate-bw-4", "queued-rpt", 4.0, 3.0, 0.5)
	if err := requireStatus(first, http.StatusOK, "queued report"); err != nil {
		t.Fatal(err)
	}
	state, _ := stringField(first.Body, "state")
	if state != "queued" {
		t.Fatalf("state: expected queued, got %q", state)
	}
	bwID, err := stringField(first.Body, "command_id")
	if err != nil || bwID == "" {
		t.Fatalf("queued report must reserve a stable fallback id: %v", err)
	}

	// 重复上报同一轮越限：身份不变，不产生第二份回退。
	replay := reportBackwater(client, "gate-bw-4", "queued-rpt", 4.2, 3.0, 0.5)
	replayState, _ := stringField(replay.Body, "state")
	replayID, _ := stringField(replay.Body, "command_id")
	if replayState != "queued" || replayID != bwID {
		t.Fatalf("replay diverged: state=%q id=%q want queued/%s", replayState, replayID, bwID)
	}

	// 闸门仍由人工命令占用，人工命令仍是 approved。
	status, aperture, active := gateState(t, client, "gate-bw-4")
	if status != "reserved" || aperture != 1.0 || active != manualID {
		t.Fatalf("manual reservation disturbed: status=%q aperture=%v active=%q", status, aperture, active)
	}
	if got, ok := commandStatus(t, client, manualID); !ok || got != "approved" {
		t.Fatalf("manual command: status=%q found=%v, want approved", got, ok)
	}
	if _, exists := commandStatus(t, client, bwID); exists {
		t.Fatalf("queued fallback command must not materialize before activation")
	}

	// 排队回退还不是命令：任何执行尝试都拿不到闸门。
	stolen := resultFor(t, client, "/commands/"+bwID+"/execute", map[string]any{
		"operator":       "attacker",
		"token":          "anything",
		"simulate_fault": false,
	})
	if err := requireStatus(stolen, http.StatusNotFound, "execute unmaterialized fallback"); err != nil {
		t.Fatal(err)
	}

	// 越限很快解除：排队回退被取消，人工流程照常执行。
	cleared := reportBackwater(client, "gate-bw-4", "queued-rpt", 2.0, 3.0, 0.5)
	clearedState, _ := stringField(cleared.Body, "state")
	if clearedState != "canceled" {
		t.Fatalf("state after clearance: expected canceled, got %q", clearedState)
	}
	status, _, active = gateState(t, client, "gate-bw-4")
	if status != "reserved" || active != manualID {
		t.Fatalf("gate after cancel: status=%q active=%q, want reserved/%s", status, active, manualID)
	}

	executed, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(executed, http.StatusOK, "manual execute after cancel"); err != nil {
		t.Fatal(err)
	}
	status, aperture, active = gateState(t, client, "gate-bw-4")
	if status != "idle" || aperture != 3.0 || active != "" {
		t.Fatalf("gate after manual execute: status=%q aperture=%v active=%q", status, aperture, active)
	}
	if _, exists := commandStatus(t, client, bwID); exists {
		t.Fatalf("canceled fallback must never materialize a command")
	}
}

// 越限持续到人工命令执行结束：人工动作完成后回退才接续，
// 两条命令都可追溯，闸门最终与回退一致。
func TestBackwaterActivatesAfterManualExecution(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-5", "回退闸五", 5, 1.0)
	manual := mustCommand(t, client, "gate-bw-5", 3.0, "人工调度", "dispatcher-lin")
	manualID, _ := stringField(manual, "id")
	approved := mustReview(t, client, manualID, "safety-wu")
	token, _ := stringField(approved, "execution_token")

	queued := reportBackwater(client, "gate-bw-5", "wait-rpt", 4.0, 3.0, 0.5)
	bwID, _ := stringField(queued.Body, "command_id")

	executed, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(executed, http.StatusOK, "execute manual with queued fallback"); err != nil {
		t.Fatal(err)
	}

	if got, ok := commandStatus(t, client, manualID); !ok || got != "completed" {
		t.Fatalf("manual command: status=%q found=%v, want completed", got, ok)
	}
	if got, ok := commandStatus(t, client, bwID); !ok || got != "completed" {
		t.Fatalf("fallback command: status=%q found=%v, want completed", got, ok)
	}
	status, aperture, active := gateState(t, client, "gate-bw-5")
	if status != "idle" || aperture != 0.5 || active != "" {
		t.Fatalf("gate after activation: status=%q aperture=%v active=%q", status, aperture, active)
	}

	// 人工命令终态后凭据失效，回退接续不改变这一保证。
	reused, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(reused, http.StatusConflict, "manual token stays invalid after fallback"); err != nil {
		t.Fatal(err)
	}

	// 越限解除在回退完成后到达：终态轮次保持 completed，不复活。
	lateClear := reportBackwater(client, "gate-bw-5", "wait-rpt", 2.0, 3.0, 0.5)
	lateState, _ := stringField(lateClear.Body, "state")
	lateID, _ := stringField(lateClear.Body, "command_id")
	if lateState != "completed" || lateID != bwID {
		t.Fatalf("terminal round changed: state=%q id=%q want completed/%s", lateState, lateID, bwID)
	}
}

// 多轮不同越限排队：人工动作结束后按序接续，每轮各有稳定身份且都完成。
func TestBackwaterMultipleQueuedRoundsDrainInOrder(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-6", "回退闸六", 5, 1.0)
	manual := mustCommand(t, client, "gate-bw-6", 3.0, "人工调度", "dispatcher-lin")
	manualID, _ := stringField(manual, "id")
	approved := mustReview(t, client, manualID, "safety-wu")
	token, _ := stringField(approved, "execution_token")

	first := reportBackwater(client, "gate-bw-6", "round-a", 4.0, 3.0, 2.0)
	second := reportBackwater(client, "gate-bw-6", "round-b", 4.0, 3.0, 0.5)
	idA, _ := stringField(first.Body, "command_id")
	idB, _ := stringField(second.Body, "command_id")
	if idA == "" || idB == "" || idA == idB {
		t.Fatalf("queued rounds need distinct identities: %q / %q", idA, idB)
	}

	executed, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(executed, http.StatusOK, "execute manual with two queued fallbacks"); err != nil {
		t.Fatal(err)
	}

	if got, _ := commandStatus(t, client, idA); got != "completed" {
		t.Fatalf("round A fallback: %q, want completed", got)
	}
	if got, _ := commandStatus(t, client, idB); got != "completed" {
		t.Fatalf("round B fallback: %q, want completed", got)
	}
	status, aperture, active := gateState(t, client, "gate-bw-6")
	if status != "idle" || aperture != 0.5 || active != "" {
		t.Fatalf("gate after draining rounds: status=%q aperture=%v active=%q", status, aperture, active)
	}
}

// 排队期间取消其中一轮：只影响该轮回退，其他轮照常接续。
func TestBackwaterCancelOneQueuedRound(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-7", "回退闸七", 5, 1.0)
	manual := mustCommand(t, client, "gate-bw-7", 3.0, "人工调度", "dispatcher-lin")
	manualID, _ := stringField(manual, "id")
	approved := mustReview(t, client, manualID, "safety-wu")
	token, _ := stringField(approved, "execution_token")

	roundA := reportBackwater(client, "gate-bw-7", "round-a", 4.0, 3.0, 2.0)
	roundB := reportBackwater(client, "gate-bw-7", "round-b", 4.0, 3.0, 0.5)
	idA, _ := stringField(roundA.Body, "command_id")
	idB, _ := stringField(roundB.Body, "command_id")

	cancelA := reportBackwater(client, "gate-bw-7", "round-a", 2.0, 3.0, 2.0)
	if state, _ := stringField(cancelA.Body, "state"); state != "canceled" {
		t.Fatalf("round A state: %q, want canceled", state)
	}

	executed, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(executed, http.StatusOK, "execute manual after partial cancel"); err != nil {
		t.Fatal(err)
	}
	if _, exists := commandStatus(t, client, idA); exists {
		t.Fatalf("canceled round A materialized a command")
	}
	if got, _ := commandStatus(t, client, idB); got != "completed" {
		t.Fatalf("round B fallback: %q, want completed", got)
	}
	if _, aperture, active := gateState(t, client, "gate-bw-7"); aperture != 0.5 || active != "" {
		t.Fatalf("gate: aperture=%v active=%q, want 0.5/empty", aperture, active)
	}
}

// 人工执行故障：闸门 faulted，排队回退不自动恢复，状态保持 queued、身份不变。
func TestBackwaterQueuedSurvivesFaultedGate(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-8", "回退闸八", 5, 1.0)
	manual := mustCommand(t, client, "gate-bw-8", 3.0, "人工调度", "dispatcher-lin")
	manualID, _ := stringField(manual, "id")
	approved := mustReview(t, client, manualID, "safety-wu")
	token, _ := stringField(approved, "execution_token")

	queued := reportBackwater(client, "gate-bw-8", "fault-rpt", 4.0, 3.0, 0.5)
	bwID, _ := stringField(queued.Body, "command_id")

	faulted, err := client.post("/commands/"+manualID+"/execute", map[string]any{
		"operator":       "operator-chen",
		"token":          token,
		"simulate_fault": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireStatus(faulted, http.StatusOK, "execute with simulated fault"); err != nil {
		t.Fatal(err)
	}
	status, _, active := gateState(t, client, "gate-bw-8")
	if status != "faulted" || active != "" {
		t.Fatalf("gate: status=%q active=%q, want faulted/empty", status, active)
	}

	replay := reportBackwater(client, "gate-bw-8", "fault-rpt", 4.5, 3.0, 0.5)
	replayState, _ := stringField(replay.Body, "state")
	replayID, _ := stringField(replay.Body, "command_id")
	if replayState != "queued" || replayID != bwID {
		t.Fatalf("queued fallback after fault: state=%q id=%q, want queued/%s", replayState, replayID, bwID)
	}
	if _, exists := commandStatus(t, client, bwID); exists {
		t.Fatalf("fallback must not auto-activate on a faulted gate")
	}

	// 解除上报可以显式取消悬挂的回退。
	canceled := reportBackwater(client, "gate-bw-8", "fault-rpt", 2.0, 3.0, 0.5)
	if state, _ := stringField(canceled.Body, "state"); state != "canceled" {
		t.Fatalf("state: %q, want canceled", state)
	}
}

// 并发重复上报同一轮越限：只有一份回退动作，闸门只移动一次。
func TestBackwaterConcurrentReportsOnIdleGate(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-9", "回退闸九", 5, 2.0)

	const n = 16
	var wg sync.WaitGroup
	ids := make([]string, n)
	states := make([]string, n)
	statuses := make([]int, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			result := reportBackwater(client, "gate-bw-9", "race-rpt", 4.0, 3.0, 0.5)
			statuses[i] = result.Status
			states[i], _ = stringField(result.Body, "state")
			ids[i], _ = stringField(result.Body, "command_id")
		}(i)
	}
	close(start)
	wg.Wait()

	winner := ""
	for i := 0; i < n; i++ {
		if statuses[i] != http.StatusOK || states[i] != "completed" {
			t.Fatalf("report %d: http=%d state=%q", i, statuses[i], states[i])
		}
		if ids[i] == "" {
			t.Fatalf("report %d missing command id", i)
		}
		if winner == "" {
			winner = ids[i]
		} else if ids[i] != winner {
			t.Fatalf("report %d created a second fallback: %q vs %q", i, ids[i], winner)
		}
	}
	if status, aperture, active := gateState(t, client, "gate-bw-9"); status != "idle" || aperture != 0.5 || active != "" {
		t.Fatalf("gate after concurrent reports: status=%q aperture=%v active=%q", status, aperture, active)
	}
	if got, ok := commandStatus(t, client, winner); !ok || got != "completed" {
		t.Fatalf("single fallback command: status=%q found=%v", got, ok)
	}
}

// 越限上报与人工 clear 复核并发：要么回退先完成（闸门空闲，复核冲突），
// 要么复核先占用（回退排队），不能同时出现互相矛盾的结果。
func TestBackwaterConcurrentAgainstManualReview(t *testing.T) {
	const runs = 20
	for run := 0; run < runs; run++ {
		client := newTestClient(t)
		gateID := fmt.Sprintf("gate-race-%02d", run)
		fixtureGate(client, gateID, "竞态闸", 5, 1.0)
		manual := mustCommand(t, client, gateID, 3.0, "人工调度", "dispatcher-lin")
		manualID, _ := stringField(manual, "id")

		start := make(chan struct{})
		var reviewStatus int
		var bwState string
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			result, _ := client.post("/commands/"+manualID+"/interlock", map[string]any{
				"verdict":  "clear",
				"verifier": "safety-wu",
				"note":     "并发复核",
			})
			reviewStatus = result.Status
		}()
		go func() {
			defer wg.Done()
			<-start
			result := reportBackwater(client, gateID, "review-race", 4.0, 3.0, 0.5)
			bwState, _ = stringField(result.Body, "state")
		}()
		close(start)
		wg.Wait()

		gateStatus, _, active := gateState(t, client, gateID)
		switch {
		case reviewStatus == http.StatusOK && bwState == "queued":
			// 人工复核先占用：闸门属于人工命令，回退排队。
			if gateStatus != "reserved" || active != manualID {
				t.Fatalf("run %d: review first but gate=%q active=%q", run, gateStatus, active)
			}
		case reviewStatus == http.StatusOK && bwState == "completed":
			// 回退先完成并释放闸门，人工复核随后正常占用：两条命令不在
			// 同一时刻占用闸门，不存在互相冲突的结果。
			if gateStatus != "reserved" || active != manualID {
				t.Fatalf("run %d: fallback first but gate=%q active=%q", run, gateStatus, active)
			}
		default:
			t.Fatalf("run %d: inconsistent race outcome review=%d bw=%q gate=%q", run, reviewStatus, bwState, gateStatus)
		}
		manualGot, _ := commandStatus(t, client, manualID)
		if manualGot != "approved" {
			t.Fatalf("run %d: manual command %q, want approved", run, manualGot)
		}
	}
}

// 非法回退开度与未知闸门返回稳定错误。
func TestBackwaterValidationErrors(t *testing.T) {
	client := newTestClient(t)
	fixtureGate(client, "gate-bw-10", "回退闸十", 5, 2.0)

	invalid := reportBackwater(client, "gate-bw-10", "bad-rpt", 4.0, 3.0, 6.0)
	if err := requireStatus(invalid, http.StatusBadRequest, "reject out-of-range fallback"); err != nil {
		t.Fatal(err)
	}
	if code, _ := stringField(invalid.Body, "code"); code != "invalid_fallback_aperture" {
		t.Fatalf("error code: %q", code)
	}

	missingGate := reportBackwater(client, "gate-missing", "bad-rpt", 4.0, 3.0, 1.0)
	if err := requireStatus(missingGate, http.StatusNotFound, "unknown gate"); err != nil {
		t.Fatal(err)
	}

	emptyReport := reportBackwater(client, "gate-bw-10", "  ", 4.0, 3.0, 1.0)
	if err := requireStatus(emptyReport, http.StatusBadRequest, "blank report id"); err != nil {
		t.Fatal(err)
	}
}
