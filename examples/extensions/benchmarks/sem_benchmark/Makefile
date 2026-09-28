# Build the Sail + Nutmeg stack for sem_benchmark on a fresh VPS and run it.
#
# Prerequisites (Ubuntu/Debian, see README.md for details):
#   curl, git, protobuf-compiler, libprotobuf-dev, clang, libclang-dev, pkg-config
#   rustup (1.97.1+), uv, python3.12 (a uv-managed interpreter is fine)
#
#   make venv          # .venv with pyspark[connect] + maturin + gfrs-poc
#   make build         # nutmeg wheel + sail host, both with RUSTFLAGS=-C target-cpu=native
#   make smoke         # tiny example-directed graph, both engines, 1 run
#   make bench DS=cit-Patents ENGINE=both       # hash joins (sail default)
#   make bench-smj DS=cit-Patents ENGINE=both   # sort-merge joins (--use-smj)
#   make clean

REPO_ROOT := $(shell git rev-parse --show-toplevel)
HERE := $(REPO_ROOT)/examples/extensions/benchmarks/sem_benchmark

VENV ?= $(REPO_ROOT)/.venv
PYTHON ?= $(VENV)/bin/python
# Respect CARGO_TARGET_DIR when the environment sets it, like cargo does.
HOST_TARGET_DIR ?= $(or $(CARGO_TARGET_DIR),$(REPO_ROOT)/target)
SAIL_BIN ?= $(HOST_TARGET_DIR)/release/sail
RUSTFLAGS ?= -C target-cpu=native
CARGO_BUILD_JOBS ?= 4

WORK_DIR ?= $(HERE)/work
RESULTS_DIR ?= $(HERE)/results
DATA_DIR ?= $(HERE)/data
MAX_MEMORY ?= 24G
PARTITIONS ?= 8
THREADS ?= 8
DS ?= example-directed
ENGINE ?= both
RUNS ?= 3
TOL ?= 1e-5

RUN_ARGS = \
	  --work-dir $(WORK_DIR) \
	  --max-memory $(MAX_MEMORY) \
	  --partitions $(PARTITIONS) \
	  --threads $(THREADS) \
	  --sail-binary $(SAIL_BIN) \
	  --results-dir $(RESULTS_DIR) \
	  --data-dir $(DATA_DIR) \
	  --tol $(TOL)

.PHONY: venv build nutmeg-wheel sail-host smoke bench bench-smj clean

venv:
	uv venv --python 3.12 $(VENV)
	uv pip install --python $(PYTHON) "pyspark[connect]==4.0.1" maturin==1.9.6
	uv pip install --python $(PYTHON) --no-deps $(REPO_ROOT)/examples/extensions/graph-algorithms/gfrs-poc

nutmeg-wheel: venv
	mkdir -p $(HERE)/target/wheels
	cd $(REPO_ROOT) && \
	RUSTFLAGS="$(RUSTFLAGS)" CARGO_BUILD_JOBS=$(CARGO_BUILD_JOBS) \
	CARGO_TARGET_DIR=$(HERE)/target/nutmeg \
	$(PYTHON) -m maturin build --release --locked \
	  --manifest-path $(REPO_ROOT)/examples/extensions/nutmeg/Cargo.toml \
	  --interpreter $(PYTHON) --out $(HERE)/target/wheels
	uv pip install --python $(PYTHON) --reinstall $(HERE)/target/wheels/sail_nutmeg-*.whl

sail-host:
	cd $(REPO_ROOT) && \
	PYO3_PYTHON=$(PYTHON) \
	RUSTFLAGS="$(RUSTFLAGS)" CARGO_BUILD_JOBS=$(CARGO_BUILD_JOBS) \
	cargo build --release --locked -p sail-cli

build: nutmeg-wheel sail-host

smoke: build
	$(PYTHON) $(HERE)/main.py \
	  --engine both --dataset example-directed --runs 1 --warmup 0 $(RUN_ARGS)

bench: build
	$(PYTHON) $(HERE)/main.py \
	  --engine $(ENGINE) --dataset $(DS) --runs $(RUNS) $(RUN_ARGS)

bench-smj: build
	$(PYTHON) $(HERE)/main.py \
	  --engine $(ENGINE) --dataset $(DS) --runs $(RUNS) --use-smj $(RUN_ARGS)

clean:
	rm -rf $(WORK_DIR) $(RESULTS_DIR)
