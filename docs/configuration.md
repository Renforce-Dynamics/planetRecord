# planetRecord: configuration and dependencies

## Two ingress profiles

| Entry | Configuration | Purpose |
| --- | --- | --- |
| `planetr record` | `pkg://planetr/data/default.yaml` or `configs/default.yaml` | Generic envelopes and declared streams |
| `planetr legacy` | `configs/legacy.yaml` or a task-owned profile | PRR1 planner records and A3DB telemetry |

Both entries support configuration layering. Their schemas are different: generic recording declares `bind`, `directory`, `streams`, optional `session_id`, `metadata`, and `queue_capacity`; legacy recording declares `version`, `onboard`, `planner`, `recording`, and optional `planetd` forwarding.

Legacy profiles require `onboard.bind_host`, `onboard.port`, `planner.endpoint` and `recording.directory`. `planet-rally` owns the rally endpoints and default flush interval. A site overlay can change only its endpoint or output directory:

```yaml
extends: pkg://planet_pingpong/data/configs/recording/rally.yaml
recording:
  directory: /data/rally/sessions
  flush_interval_s: 0.25
```

A cross-package `extends` requires that package installed; the generic recorder does not otherwise depend on the task.

## Paths and sessions

Relative output directories are resolved against the process working directory. Use absolute directories for deployed services. Abstract Unix endpoints beginning with `@` are socket addresses, not paths. `planetr legacy --dir PATH` overrides the legacy output directory for one run.

`planetr record --check` validates the top-level configuration contract. `planet-rally/scripts/record.sh --check` invokes the legacy loader and checks required sections without binding sockets. The running service validates envelope schemas and session writes.

`complete` records whether errors or gaps were detected among received records. UDP cannot prove that every sent packet arrived. Replay preserves payloads; timed replay requires a single producer clock domain. Application-specific transforms write separate derived sessions.

## Source installation

Bootstrap installs `cadence-config` from the pinned `external/cadence`, then the local format, client and recorder packages. It does not install Cadence runtime, a planner or kinematics.

## Layering rules

All service configuration entry points use `cadence-config`; each service validates its own schema after composition.

1. Apply `extends` entries in their listed order.
2. Apply `compose` layers in the fixed order `robot`, `backend`, `task`, `site`, `experiment`.
3. Merge the current file.
4. Apply explicit entry-point overrides, where supported.

Mappings merge recursively; lists and scalars replace. Missing parents, duplicate YAML keys and inheritance cycles fail. A service may reject fields that are valid for a different service. `compose` keys are loader directives, not fields added to the resulting service configuration.

Relative inheritance paths resolve beside the YAML declaring them. `pkg://package/path` resolves installed package resources. Resource fields accessed through `ResolvedConfig.path()` resolve relative to their declaration; output directories and Linux device/abstract-socket endpoints follow the consuming service's rules below. The generic loader does not rewrite every string into a filesystem path.

Source ownership, Python dependencies and YAML inheritance are separate: Git submodules select code revisions; package metadata selects compatible installed distributions; `extends` selects configuration values. Changing a Git submodule does not select a task profile automatically.

See the [shared loader reference](https://github.com/Renforce-Dynamics/cadence/blob/main/docs/configuration.md).
