# planetRecord

**Robot recording, asynchronous capture and replay.**

planetRecord stores producer-defined streams and schemas in versioned sessions. A bounded asynchronous client keeps network and disk work out of control cycles; recording tracks dropped data, errors and sequence gaps.

## Quick start

Requires Linux, Python 3.10+ and `uv`.

```bash
git clone --recurse-submodules git@github.com:Renforce-Dynamics/planetRecord.git
cd planetRecord
./scripts/bootstrap.sh
./scripts/doctor.sh
./scripts/run.sh -- --duration-s 1
./scripts/test.sh
```

## Packages and dependencies

| Package | Responsibility |
| --- | --- |
| `planetr-format` | Versioned envelopes and format contracts |
| `planetr-client` | Bounded asynchronous producer client |
| `planetr` | Session storage, ingress and replay |

The [planetConfig](https://github.com/Renforce-Dynamics/planetConfig) submodule supplies `planet-config`. Bootstrap installs that loader and the three local packages. The complete source and package dependency graph is independent of any robot runtime or SDK.

## Configuration and usage

```bash
.venv/bin/planetr record --config configs/default.yaml --check
.venv/bin/planetr replay recordings/SESSION
.venv/bin/planetr legacy --config configs/legacy.yaml --duration-s 1
```

The generic `record` schema declares `bind`, `streams` and `directory`. The compatibility `legacy` schema declares `onboard`, `planner` and `recording` for PRR1/A3DB ingress. Both use `planet-config` for `extends`, `compose` and package resources; these are distinct ingress schemas.

Relative output directories are relative to the process working directory. Use an absolute output path for deployments. See [configuration and session semantics](docs/configuration.md).

Sessions contain `meta.json`, stream JSONL files and recording statistics. `complete` reports no detected loss within the received data; UDP has no delivery acknowledgement. Timed replay requires one producer clock domain. Business transforms such as racket FK belong to the consuming application; `cadence-rally derive` writes derived data separately.

## Development

```bash
./scripts/submodules.sh init    # initialize or restore pinned dependencies
./scripts/submodules.sh check
./scripts/test.sh
./scripts/build.sh
```

Submodules pin source commits; Python requirements describe package compatibility. Bootstrap installs only the explicit packages in `source-workspace.json`. `scripts/setup.sh --wheelhouse /path/to/wheels` is available for package-based installation. Upgrade dependencies by committing reviewed submodule revisions with the parent repository.

Tool defaults can be configured with `PLANET_PYTHON`, `PLANET_VENV` and `PLANET_WHEELHOUSE`, or with the corresponding command-line options.

## Authorship and license

Developed and maintained by [Renforce Dynamics](https://github.com/Renforce-Dynamics). See [AUTHORS.md](AUTHORS.md). Project code is available under the [MIT License](LICENSE).
