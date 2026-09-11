# PlanetR

通用录制服务，stream 和 schema 由生产者与配置声明。磁盘写入与控制周期隔离；异步客户端队列有界，丢弃和错误可计数。

```bash
./scripts/setup.sh --wheelhouse /path/to/wheels
./scripts/doctor.sh
./scripts/run.sh -- --config configs/default.yaml
.venv/bin/planetr replay recordings/SESSION
./scripts/test.sh
./scripts/build.sh
```

三个包分别发布：`planetr-format` 定义版本化 envelope，`planetr-client` 提供异步发送，`planetr` 保存 session 并回放。均不依赖 planner、运动学、ONNX、MuJoCo 或 NumPy。

```python
from planetr_client import RecordClient, RecordEnvelope
client = RecordClient(capacity=256)
client.publish(RecordEnvelope("demo", "robot/state", "robot.state.v1",
                             "controller", 0, 1234, {"q": [0.0, 0.2]}))
client.close()
```

每个 session 包含 `meta.json`、各流 JSONL 与 `recording_stats.json`。schema 不符会拒绝，间断或写入错误使 session 标记 incomplete。`complete` 表示接收范围内没有已检测的损失或错误；UDP 不提供端到端送达确认。回放保留原始 payload；有多个生产者时，不假定它们的单调时钟可直接排序。`--speed` 的定时回放要求单一生产者时钟域。

`planetr legacy --config configs/legacy.yaml` 接收保留的 A3DB / PRR1 协议，仅做协议适配及存储。A3 球拍 FK 派生处理在应用命令 `cadence-rally derive SESSION DESTINATION` 中，输出独立目录，不改写原始 session。
