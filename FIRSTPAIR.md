slug: sail-declarative-pipelines
shelf: querygraph
default_edition: full

# FirstPair Library Contract

This file is read by the FirstPair publisher; keep the key-value header above simple and
unbulleted.

## Ownership

This repository owns two First Pair deliverables under one title, *Sail Declarative Pipelines*:

- **The manual**, the title itself: `docs/books/sail-declarative-pipelines-manual/`
  (chapters, metadata, cover and headboard art, styles, `prepare.sh`), built by the root
  `book.build.json`. Both lanes write chapters; the number prefix orders them.
- **The technical report**, published as the title's `report` version:
  `docs/books/sail-declarative-pipelines/` (manuscript, metadata, Typst cover, styles), built
  by `docs/books/sail-declarative-pipelines/book.build.json` with stem
  `sail-declarative-pipelines-report`.

The runnable demo both describe lives in `examples/declarative-pipelines/`.

The FirstPair repository at `~/src/firstpair` owns the unified builder, the public catalog,
hosted readers, Blob uploads and deployment. Follow `~/src/firstpair/AGENTS.md`.

## Build

Commit the sources first: the builder stamps the Git HEAD into each edition.

```sh
repo="$(git rev-parse --show-toplevel)"
"$HOME/src/firstpair/publishing/scripts/build-library-book.sh" --repo-root "$repo"
"$HOME/src/firstpair/publishing/scripts/build-library-book.sh" --repo-root "$repo" \
  --config docs/books/sail-declarative-pipelines/book.build.json
```

The report's cover is rendered from `docs/books/sail-declarative-pipelines/cover/cover.typ`:

```sh
cd docs/books/sail-declarative-pipelines
typst compile --ppi 200 cover/cover.typ cover/sail-declarative-pipelines-cover.png
```

The manual's cover and headboard are supplied artwork under
`docs/books/sail-declarative-pipelines-manual/art/`.

## Publish

Publishing to firstpair.org is a separate, outward-facing step that needs the owner's explicit
go-ahead. Publish the title (the manual) first, then the report as its version:

```sh
cd "$HOME/src/firstpair"
npm run library:publish -- /path/to/this/repo \
  --title "Sail Declarative Pipelines" --dry-run --no-build --no-smoke --no-deploy --no-icloud
npm run library:publish -- /path/to/this/repo/docs/books/sail-declarative-pipelines/dist \
  --version report --version-label "Technical report" --dry-run --no-build --no-smoke --no-deploy --no-icloud
```

Run the same commands without the dry-run flags to publish.
