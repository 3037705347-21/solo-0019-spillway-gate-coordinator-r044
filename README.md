# 泄洪闸门操作协调服务

这是一个无外部依赖的 Go 后端，用本地内存仓库协调闸门登记、开度调整命令、安全联锁复核和执行结果。服务强调单闸门活动命令互斥、原子状态转换、执行令牌生命周期和稳定错误传播。

## 运行

```sh
go build ./...
go run ./cmd/server --addr 127.0.0.1:18119
```

健康入口为 `GET http://127.0.0.1:18119/health`。

## API

```text
POST /gates
GET  /gates/{id}
POST /commands
GET  /commands/{id}
POST /commands/{id}/interlock
POST /commands/{id}/execute
POST /gates/{id}/backwater
```

登记闸门示例：

```json
{
  "id": "gate-east",
  "name": "东侧泄洪闸",
  "max_aperture": 4.5,
  "initial_aperture": 0.5
}
```

提交命令示例：

```json
{
  "gate_id": "gate-east",
  "target_aperture": 2.25,
  "reason": "上游水位调整",
  "requested_by": "dispatcher-lin"
}
```

联锁复核示例：

```json
{
  "verdict": "clear",
  "verifier": "safety-wu",
  "note": "上下游条件满足"
}
```

执行示例：

```json
{
  "operator": "operator-chen",
  "token": "复核响应中的 execution_token",
  "simulate_fault": false
}
```

## 下游水位联动

`POST /gates/{id}/backwater` 接收一轮下游水位上报：

```json
{
  "report_id": "incident-2026-10-04-01",
  "level": 4.2,
  "limit": 3.0,
  "fallback_aperture": 0.5
}
```

联动规则：

- 一轮越限由 `(闸门, report_id)` 唯一标识，重复上报返回同一轮结果，不会生成第二份回退动作或命令。
- 闸门空闲时，自动回退作为一条带联锁结论和执行令牌的普通命令完成（`completed`），闸门经过 reserved/moving 后回到 idle。
- 闸门已被人工命令占用（reserved/moving/faulted）时，回退以稳定的命令身份进入 `queued`，绝不覆写在途人工命令；人工动作正常结束回到 idle 后排队回退才接续执行。
- 同一轮随后收到未越限上报：排队中的回退转为 `canceled`（令牌失效、永不物化）；已完成的轮次保持 `completed` 不变。
- faulted 闸门不参与自动恢复，排队回退保持 `queued` 直至显式解除。

## 目录

- `cmd/server`：服务进程。
- `cmd/workflowcheck`：四条工作流的真实 HTTP 检查入口。
- `internal/bootstrap`：依赖装配。
- `internal/contracts`：公开数据契约。
- `internal/coordination`：业务工作流。
- `internal/domain`：实体、不变量和状态机。
- `internal/httpapi`：路由、解码和错误映射。
- `internal/storage`：互斥保护的内存仓库。

## 工作流检查

```sh
go run ./cmd/workflowcheck --workflow register-gate
go run ./cmd/workflowcheck --workflow request-gate-command
go run ./cmd/workflowcheck --workflow review-safety-interlock
go run ./cmd/workflowcheck --workflow execute-gate-command
go run ./cmd/workflowcheck --workflow backwater-interlock
```

这些检查会创建临时 HTTP 服务并调用真实路由。它们不调用外部网络，也不依赖数据库。

## 测试状态

测试有意延后。当前交付仅包含初始化基线和生产级 smoke 检查；后续任务阶段会添加自动化测试与红绿验证，不应把当前没有测试文件视为项目未完成。
