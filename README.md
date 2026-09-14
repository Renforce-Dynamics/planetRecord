# planetRecord

录制生产者定义的数据流，保存会话、统计信息并按时间回放。

## 安装与录制

需要 Linux、Python 3.10+、`uv`。

```bash
git clone --recurse-submodules git@github.com:Renforce-Dynamics/planetRecord.git
cd planetRecord
./scripts/bootstrap.sh

./scripts/run.sh --config configs/entry/entry_recorder.yaml
```

默认监听 `127.0.0.1:50571`，输出到 `recordings/`。生产者使用
`planetr-client` 发送带流名和 schema 的数据；Ctrl+C 停止并完成会话写入。

## 选择数据流与输出位置

新建 `configs/entry/entry_recorder_site.yaml`：

```yaml
extends: entry_recorder.yaml
bind:
  host: 0.0.0.0
  port: 50571
directory: /data/recordings
```

```bash
./scripts/run.sh --config configs/entry/entry_recorder_site.yaml
```

`streams` 声明流名、schema 和 JSONL 文件名。按需覆盖默认流，
输出目录建议使用绝对路径；配置字段见 [配置文档](docs/configuration.md)。

## 回放与旧协议录制

```bash
# 按原始时间回放到标准输出；--speed 0 为尽快输出
.venv/bin/planetr replay recordings/SESSION --speed 1

# PRR1 / A3DB 兼容入口，使用独立的 legacy 配置
.venv/bin/planetr legacy --config configs/entry/entry_legacy_recorder.yaml
```

将 `SESSION` 替换为实际会话目录。会话包含 `meta.json`、各流 JSONL 和录制统计。
通用 record 与 legacy 的输入格式不同；UDP 统计不等同于传输送达确认。

配置只保存在根目录 `configs/`；安装包只包含代码。每次启动都显式选择 entry，缺失路径直接报错。

## 包与开发

包含 `planetr-format` 数据格式、`planetr-client` 异步客户端和 `planetr` 录制服务。

仅通过 [planetConfig](https://github.com/Renforce-Dynamics/planetConfig) 复用配置库，
不依赖 Cadence 或 SDK。业务派生计算由使用记录的应用维护。

开发：`./scripts/test.sh` 运行测试，`./scripts/build.sh` 构建安装包。
`./scripts/doctor.sh --config configs/entry/entry_recorder.yaml` 检查选定配置，不启动 I/O。

工具支持 `--venv /path/to/env`。由 **Renforce Dynamics** 开发维护，采用 [MIT License](LICENSE)。
