# Linux extension build environment on Morrobay

This is a development-environment recovery record, not a build or test verdict.
The [follow-up plan](datafusion-graph-plan.md) records candidate qualification.

## Verified environment

The separate Colima profile `sail-gate` uses QEMU 11.1.1 through Colima 0.10.3,
with x86_64 virtualization. Its requested resources are 36 CPUs, 80 GiB memory,
a 180 GiB Docker data disk and a separate 20 GiB root disk. The host has 128 GiB
physical memory. A 9p mount exposes only
`/Users/alexy/src/sail-extensions-gates` for this task.

Guest measurements at `2026-09-26T05:04:59Z` were:

| Measurement | Observed |
| --- | --- |
| Kernel | Linux 6.8.0-117-generic, x86_64 |
| `/proc/meminfo` `MemTotal` | 82,342,536 KiB, approximately 78.53 GiB |
| `nproc` | 36 |
| Docker-reported memory | 84,318,756,864 bytes |
| Root filesystem | 19 GiB total, 18 GiB available |
| Docker data filesystem | 177 GiB total, 168 GiB available before imports |
| Host filesystem available | 265 GiB at that measurement |

The development container uses a 72 GiB memory limit and no additional swap,
leaving VM headroom, with `CARGO_BUILD_JOBS=18` and `CARGO_INCREMENTAL=0`.
Its cgroup reports `memory.max=77309411328`, `memory.swap.max=0` and
`cpu.max=3600000 100000`. It has Rust/Cargo 1.97.1 and Python 3.12.14. After cache
imports, the Docker data filesystem had 158 GiB available and the host had
255 GiB available (`2026-09-26T05:10:16Z`).
Disk availability is a point-in-time observation; recheck it before long builds.
This shared host is not a basis for publishing dedicated-host performance timings.

## Recovery provenance

The initial Linux development build used the existing default VZ profile. During
setup its configuration was changed from 12 CPUs/48 GiB/120 GiB disk to
36 CPUs/96 GiB/220 GiB disk. A subsequent guest measurement reported only about
3 GiB memory, despite that configuration, and the development build encountered
killed compiler processes. Guest memory was not measured before the change;
this observation says nothing about the resource envelope of earlier benchmarks.

Only this task's image and volumes were exported while its container was paused:

- `sail-extensions-gate:rust1.97.1`, image ID
  `sha256:5f3470ae8f4c76f754a054c44a15cfb5a96935ece5bc208bddde6e2b6554a42d`;
- `sail-extension-targets`;
- `sail-extension-cargo-registry`.

Exports and recovery logs remain under
`/Users/alexy/src/sail-extensions-gates/qemu-migration/` on Morrobay. They include
`image.tar`, `targets.tar`, `cargo-registry.tar`, `export-state.txt`,
`guest-resources.txt`, `start.log` and the original default Colima configuration.
These are local artifacts, not repository files. The original default VM was
stopped without deleting its images, volumes or disk. Its configuration was then
restored to 12 CPUs/48 GiB, retaining the enlarged 220 GiB disk without shrinking
or restarting it. The new profile is selected explicitly, not made the active
Docker context.

## Reproduction and use

QEMU is an additional prerequisite for this profile. From Morrobay, with
`$HOME/.cargo/bin` and `/usr/local/bin` on `PATH`:

```bash
colima start --profile sail-gate --vm-type qemu --arch x86_64 \
  --cpu 36 --memory 80 --disk 180 --mount-type 9p \
  --mount /Users/alexy/src/sail-extensions-gates:w --activate=false

colima ssh --profile sail-gate -- sh -c \
  'grep MemTotal /proc/meminfo; nproc; df -h / /var/lib/docker' </dev/null
docker --context colima-sail-gate info
```

The `</dev/null` matters when this is run inside a script delivered through SSH
standard input: nested SSH must not consume the rest of that script.

The migration uses `docker save`/`load` for the named image and read-only volume
exports through `tar`. Imports populate the same volume names in the new Docker
context. Neither global prune nor deletion of the original VM is needed.

The local artifact `qemu-migration/create-development-container.sh` creates
`sail-extension-development` in `colima-sail-gate`, mounting the gate directory
at `/work`, the target volume at `/targets`, and the registry volume at
`/usr/local/cargo/registry`. Its source working directory is `/work/repository`.
It exports `SAIL_GATE_IMAGE_ID` and
`SAIL_GATE_VM_PROFILE` for receipts, plus the build concurrency/memory settings.
The container's Git configuration trusts only the two task repositories at
`/work/repository` and `/work/repository/examples/extensions/sedona/.deps/sedona-db`,
whose host UID differs from the container's root user.

## Keep source and Python files on the Linux filesystem

The first QEMU development attempt built and installed both native wheels,
including the Sedona bundled-library check. Host compilation then spent several
minutes in Cargo's `gix::dirwalk` thread: `/proc` showed the thread waiting in
`p9_client` with source directories open under the host mount. No Rust compiler
had started, and cgroup OOM counters remained zero. Its interrupted log is
`linux-development-qemu-build-9p-attempt1.log` on the host.

The follow-up copies the development repository, including `.git` and pinned
Sedona `.deps`, plus its Python environment, into `/targets/linux-snapshot/`.
The copy is streamed from a host-native `tar` through Docker stdin, avoiding a
guest directory walk over 9p. The source becomes
`/targets/linux-snapshot/repository`; the relocated environment becomes
`/targets/linux-snapshot/linux-venv`. Python launcher paths must be updated when
relocating a venv, or the environment can be recreated there from the lock file.
The target stays `/targets/linux-development`. This control reached host Rust
compilation promptly. It then exposed a missing build prerequisite:
`protobuf-compiler` alone did not install `google/protobuf/any.proto`.
Installing `libprotobuf-dev` supplied that file. The failed attempt is retained
as `linux-development-qemu-build-native-attempt2.log`; the subsequent build
outcome belongs to its log, not to the successful native-wheel results.

After synchronizing/copying the intended source, run the native-filesystem
development build with explicit overrides (the container's original default
working directory and venv still refer to `/work`):

```bash
docker --context colima-sail-gate exec \
  --workdir /targets/linux-snapshot/repository \
  --env SAIL_EXTENSION_VENV=/targets/linux-snapshot/linux-venv \
  --env PYO3_PYTHON=/targets/linux-snapshot/linux-venv/bin/python \
  sail-extension-development \
  bash examples/extensions/scripts/build.sh \
  > /Users/alexy/src/sail-extensions-gates/linux-development-qemu-build.log 2>&1
```

The image is built from the checked-in
[Dockerfile](../../../examples/extensions/scripts/Dockerfile.linux). Build
artifacts, Python packages and source traversal now belong on Linux-native
storage. Docker stdout is redirected by the host shell, rather than writing a
guest log through 9p; copy other receipts/logs out after the run.
The final image also includes `libprotobuf-dev`, stable `rustfmt`/`clippy`
components, and the pinned `nightly-2026-05-28` formatter required by verification.
Stable rustfmt is also invoked by Sail's HMS build script; the minimal Rust image
did not include it. Rebuild the image from the
candidate Dockerfile and record its actual image ID; the migrated image identity
above describes the initial recovery image, before these prerequisite fixes.

## Formal candidate gate

A formal gate uses an immutable detached worktree and its own target directory.
Transfer the frozen commit in a Git bundle through Docker stdin into
`/targets/linux-gates/`. Clone its Git objects into a native bare repository and
create the detached worktree from inside Linux, for example:

```bash
# Inside the container; CANDIDATE_SHA is supplied by the frozen-candidate receipt.
git clone --bare /targets/linux-gates/candidate.bundle \
  /targets/linux-gates/repository.git
git --git-dir=/targets/linux-gates/repository.git worktree add --detach \
  /targets/linux-gates/candidate "$CANDIDATE_SHA"
```

Creating that worktree inside Linux keeps its absolute Git metadata paths valid.
A host-created worktree's `.git` pointer would otherwise name an absent
`/Users/alexy/...` path inside the container. Use a separate candidate target and
venv under `/targets/linux-gates/`, then run the repository's build/verification
scripts. A compiler cache may be seeded only after all development writers stop;
the gate must still build and verify the frozen source in its own target. Do not
promote a mutable development checkout's outcome to a candidate verdict.

Record actual guest/cgroup memory, CPU, disk, image identity and profile alongside
the exact commit. A configured VM limit alone is insufficient evidence.

After final evidence is copied out, stop this task's container and `sail-gate`
profile, then restart the user's default profile with its restored settings.
Keep the existing disk/images/volumes; no prune or VM deletion is required.
