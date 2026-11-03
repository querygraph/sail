# Proposed maintainer request: a bounded native extension contract

Draft for review; not posted upstream. This is the single proposed discussion
entry. The [design review](design-review.md), [implemented follow-up](review-follow-up.md)
and [expanded ABI experiments](abi-review.md) provide detail. Proposal v5 supplies
prior constraint research, not a second ask.

We built two independently packaged native extensions on a Sail branch: SedonaDB
scalars executing on workers, and Nutmeg stateful relations executing on the
driver. Ordinary graph-table queries use existing Sail/DataFusion plans and work
without enabling native extensions. Neither domain library is a Sail engine
dependency. We are asking about a small host contract, not proposing the complete
PoC branch for merge.

Your requirement that Sail and extensions release independently is central. The
loader currently checks API/DataFusion/Arrow versions rather than Sail commit or
Rust compiler identity. The follow-up tests unchanged wheels against distinct
host revisions, records their artifact hashes and rejection tests, and scopes
support to that measured matrix. This is evidence of reuse within that matrix,
not a stable ABI promise across arbitrary DataFusion or Rust releases. The next
step toward broader support is explicit compatibility-version negotiation backed
by unchanged-wheel CI, rather than relaxing version checks speculatively.

The proposed host responsibilities are:

- Discover and retain trusted native implementations, reject collisions and known
  incompatibilities, and resolve matching installed scalar implementations on workers.
- Dispatch bounded Connect relation payloads with ordinary child plans and pure
  planning; retain the actual host runtime and resource policies for host inputs.
- Keep driver-local stateful regions on the driver and disable automatic replay
  of those regions, reporting an unacknowledged mutation as indeterminate.
- Provide a versioned opaque quota lease with final-owner accounting. A session
  manager explicitly owns and injects the admission pool into its sessions and
  in-process workers. Equal configuration never implies shared ownership.

The foreign region remains opaque to host optimization. We do not claim general
foreign-service propagation, arbitrary sorted/co-partitioned input requirements,
driver-only scalar scheduling, indexed spatial joins or distributed native CSR
iteration. Native allocations are non-spillable and participating reservations
are not a process RSS cap. Python discovery is a bootstrap choice; it does not
expose Rust trait objects as the extension ABI.

We propose independent lifecycle and field-metadata correctness PRs first, then
scalar registration/distribution, then bounded relation dispatch, resource leases
and driver placement. Parser depth/stack growth is a separately reviewed
prerequisite. Domain packages, wheel repair and deployment harnesses stay outside
the engine contract. Each extracted commit and combined head receives new gates.

Field semantics remain an explicit, independent review decision: the metadata
changes add geometry-specific compatibility/coercion rules to Sail core even
though neither domain library is a core dependency. Agreement to the loader
contract does not implicitly approve those rules. The remaining open choices
are enumerated in the design review.

The decisions we need are whether this experimental compatibility policy and
bounded relation interface are acceptable, whether the session manager is the
right default admission domain, and what graceful-shutdown policy should apply
to noncooperative native owners. The exact tested matrix and redacted, checksummed
receipts accompany the follow-up. The original two-host result uses Rosetta on
one host and establishes functionality only.

Prior maintainer constraints and Connect discussion are in
[Sail discussion #2001](https://github.com/lakehq/sail/discussions/2001), including
[FFI and independent releases](https://github.com/lakehq/sail/discussions/2001#discussioncomment-17083578).
