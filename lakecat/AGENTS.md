# AGENTS.md — working guidance for the LakeCat fork of Sail

This is a fork of [Sail](https://github.com/lakehq/sail) used for **LakeCat**
development. AI coding agents (Claude Code, Copilot, etc.) and contributors
working in this repo must follow the guidance below.

The **authoritative** engineering instructions live upstream and still apply:

- [`../.github/copilot-instructions.md`](../.github/copilot-instructions.md)
- [`../.github/instructions/`](../.github/instructions/) — `dev`,
  `rust-review`, `python-review`

**Read those before writing code or opening a PR.** This file does not replace
them; it distills maintainer guidance into working discipline that agents have
repeatedly gotten wrong, and points back to the source of truth.

## 1. Pull-request discipline

Maintainer guidance (Heran), to be followed literally:

- **State the intention.** Every PR must have a clear, stated purpose. Do not
  open uncoordinated, purely AI-generated PRs without manual design review.
- **Coordinate on internals.** The team is moving rapidly on catalog and table
  operations and other deep internals. Changes to shared surfaces — the
  `CatalogProvider` trait, catalog/table ops, Iceberg internals — must be
  **coordinated with existing efforts before** a PR is opened. Uncoordinated
  changes here are hard to land and create confusion.
- **Mark prototypes as such.** If you are sharing exploratory work, open the PR
  as **draft**, or **close it right after creation with a note** describing the
  intention. Do not leave ambiguous open PRs.
- **Merge-bound code follows the instructions.** Code intended for `main` must
  follow `../.github/copilot-instructions.md` and `../.github/instructions`. A
  PR-title CI failure is a signal the instructions were not followed.

## 2. PR / commit titles (this is what fails CI)

Per `dev.instructions.md` → *Contributing*. Conventional Commits:
`<type>: <description>`.

- `<type>` ∈ `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, …
- **Do not add a `<scope>`.** A scope is allowed *only* for dependabot PRs.
  So `fix(iceberg): …` and `feat(catalog): …` are **invalid** — use `fix: …`,
  `feat: …`.
- `<description>` starts lowercase; refer to code elements sparingly; rephrase to
  avoid a leading word that must be capitalized (e.g. "SQL").

## 3. Design before code on shared surfaces

Light design discussion with the owners **precedes** code changes to shared
traits or deep internals — bring a proposal, not a finished diff.

> Worked example: a `commit_table` method was added to `CatalogProvider` while
> `commit_lakehouse_table` (Xiaolong's) already covered that path — a redundant
> addition a short design discussion would have caught. Reconcile with what
> exists before adding to a shared surface.

## 4. Express gaps as BDD `.feature` tests

Per `dev.instructions.md` → *Test Style*: Sail prefers **BDD `.feature` tests
(Gherkin)** that assert **user-facing SQL outcomes**, not internal state.

- definitions: `python/pysail/tests/**/*.feature`
- loaders: `python/pysail/tests/**/test*_features.py`
- steps: `python/pysail/testing/**/steps/*.py`

When a change fills a behavioral gap (e.g. Iceberg manifest pruning, table commit
semantics), **express the gap as a `.feature` scenario** so reviewers can see how
the change affects real SQL usage and decide the best path forward. Rust tests
are strongly discouraged except for internal utilities with easily constructed
inputs and straightforward outputs.

## LakeCat fork specifics

- LakeCat-targeted changes to Sail live on the **`lakecat`** branch; see
  [`../LAKECAT-SAIL.md`](../LAKECAT-SAIL.md) for the catalog & Iceberg change log
  and upstream-coordination status.
- The fork is consumed by `~/src/lakecat` via a git dependency pinned to this
  branch; keep the branch rebased onto upstream `main` as it moves.
