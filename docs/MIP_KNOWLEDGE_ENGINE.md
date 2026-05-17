# MIP Knowledge Engine

Status: early design note. This captures the broader direction in which MIP
stops being only a hierarchical summarizer and becomes a general local
knowledge substrate. Keep this document small until the strong concepts become
clear through use.

## Naming The Atom

Current decision: **fact** is the atomic node.

This is not an epistemological claim that the content is true. In this system,
a fact is the smallest addressable unit of information the engine can store,
index, relate, summarize, and reason over. It may be emitted by an app,
asserted by a human, measured from a stream, extracted from a blob, inferred by
a model, or derived from other facts.

Truth, confidence, source, and provenance are fields on a fact, not guarantees
implied by the word.

Working distinction:

- **Blob:** uninterpreted payload bytes or text, usually content-addressed.
- **Fact:** an atomic knowledge record with provenance.
- **Relation:** a fact whose value points at another fact or subject.
- **Summary:** a derived fact that compresses other facts.
- **View:** an organizing projection over facts.
- **Signal:** contextual salience, correlation, or usefulness discovered among
  facts.

This makes `signal` an analytic layer rather than the atom. A fact enters the
store before the engine knows whether it is signal or noise. A signal is what
emerges when facts become relevant under a query, task, view, or correlation
recipe.

Other atom names considered:

- **Observation:** precise, but too passive and too tied to a human/scientific
  observer.
- **Signal:** useful, but it already implies relevance or signal-vs-noise.
- **Trace:** excellent for provenance, but less natural for intentional
  assertions and derived summaries.
- **Evidence:** strong epistemology, but implies argument or proof too early.
- **Sample:** good for the manifold analogy, but too numerical/statistical.
- **Record:** practical, but too storage-shaped.
- **Datum:** accurate, but sterile and too low-level.

Avoid treating this as final vocabulary. The useful constraint is that the
atom must be general enough for natural-language history, app-native structure,
structured assertions, derived summaries, and non-textual event streams.

## Core Framing

MIP should not own a single canonical hierarchy. App-native hierarchies,
semantic clusters, timelines, derivation chains, and user-curated context
paths are all projections over the same underlying facts.

The substrate is therefore not a pure tree and not only a DAG. It is a
temporal, attributed knowledge graph with DAG-shaped projections where those
projections are useful.

The clearest split:

- The **knowledge engine** stores blobs, facts, provenance, indexes, and
  derived views.
- The **MIP engine** creates level-of-detail summaries, signals, and other
  derived facts over that substrate.
- Hyperion's **context engine** builds acyclic context projections from the
  substrate for model input.

This keeps Hyperion focused on curation and linearization while allowing the
knowledge substrate to remain cyclic, temporal, uncertain, and cross-domain.

## Minimal App Contract

Apps should be able to stream information into the system without adopting a
large ontology. The app-side contract should stay close to these verbs:

- **record:** submit a fact with provenance, time, content or value, and
  optional metadata.
- **assert:** attach a structured fact to a subject.
- **relate:** declare a relationship between subjects.
- **view:** provide an app-native organization, ordering, or membership.
- **checkpoint:** mark ingestion progress.

The engine can then emit derived material back:

- summaries
- correlation candidates
- derived facts
- signals, meaning scored salience or usefulness under a context
- derived views
- drift or conflict notices
- context candidates

The important property is bidirectionality: apps can publish their native
structure, but the central engine can suggest cross-app structure without
rewriting app truth.

## First Dogfood Adapter: Git

Git is a strong first app-side adapter because it already provides deeply
structured, content-addressed, graph-shaped data. A thin Git adapter can start
by recording commits, parent edges, refs, trees, changed paths, and patch
payloads as facts while preserving Git as the authority for native
connectivity.

This should stay a glue layer. The adapter observes Git's own structure and
publishes it into the knowledge engine; MIP then builds derived summaries,
views, and signals over that source material.

Git also tests the hard part of the multiscale idea: useful work often hides
inside long commit sequences, merges, mixed-purpose commits, and broad change
sets. The engine should make it possible to navigate from a high-level
history summary down through branches, commit ranges, individual commits,
files, hunks, and source blobs without losing provenance.

Potential seam-finding is deliberately framed as derived analysis, not as a
replacement for Git truth. A commit may remain one Git commit while MIP
detects that it contains several conceptual changes, or that a concept spans
several commits.

## Property Classes

The first reusable abstraction should be property classes, not app object
classes.

A property class describes how a field participates in correlation:

- canonical name
- aliases
- value kind
- normalization rules
- available indexes
- comparison operators
- default salience

For example, short labels like `desc`, `description`, and `summary` may
eventually map to one text-bearing property class. The exact class names are
less important than the mechanism: fields become relation surfaces once the
engine knows how to normalize and compare them.

This supports the core intuition that arbitrary data becomes correlatable when
it shares fields. Some correlations are weak, such as merely sharing a
property. Others become strong when normalized equality, temporal proximity,
embedding similarity, provenance, and user curation reinforce each other.

## Graph Shape

Use multiple graph semantics over shared identifiers:

- **Provenance DAG:** what was derived from what.
- **Native views:** how an app says its world is organized.
- **Knowledge graph:** facts and relations, including cycles.
- **Temporal indexes/views:** ordering, duration, cadence, and buckets.
- **Context projections:** acyclic selections prepared for model input.

The provenance layer should be acyclic. The knowledge layer should not be
forced to be acyclic, because useful knowledge often loops back on itself.

## Multiscale Visualization

Visualization should be treated as a base capability of the knowledge engine,
not just a later UI feature. The engine should be able to provide multiscale
views that preserve enough structure for a renderer to show where a user is in
the data and how far they are zoomed in.

For Git, this means the native DAG should be table stakes: commits and parent
edges are visible at full fidelity. MIP-derived layers can then add summaries,
topic bands, time buckets, conceptual seams, hot paths, and navigation hints
above that DAG.

The goal is not to replace low-level inspection. The goal is to make the
highest useful abstraction level accessible first, then allow precise descent
only where the task demands it.

## Design Constraints

- Raw facts and blobs must remain available even when summaries exist.
- Derived facts must carry provenance back to source facts.
- Apps should not need to predict future global ontology decisions.
- The engine should support dense data that is rolled up or indexed rather
  than summarized as prose.
- Time should be first-class because it is the universal cross-app correlator.
- Human or agent curation should become facts that can act as high-value
  signal, not hidden UI state.
- A weak correlation should be cheap to record and easy to supersede.
- A strong correlation should be explainable through supporting facts and
  source facts.
- App-native connectivity should remain authoritative even when MIP derives
  alternate seams, clusters, or summaries.
- Multiscale views should let users descend from summary to source without
  severing provenance.

## Open Questions

- Is **fact** too epistemologically loaded despite the operational definition?
- Should every relation be represented as a fact from day one?
- How much property-class normalization should happen at ingestion time versus
  asynchronously?
- What is the smallest query language that can express correlation salience
  without becoming a premature ontology language?
- Where should MIP stop and Hyperion's context engine begin?
- Should visualization-oriented view data be stored as facts, generated on
  demand from indexes, or cached as derived views?
