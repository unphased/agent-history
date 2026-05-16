# MIP Knowledge Engine

Status: early design note. This captures the broader direction in which MIP
stops being only a hierarchical summarizer and becomes a general local
knowledge substrate. Keep this document small until the strong concepts become
clear through use.

## Naming The Atom

The previous working word was **observation**: something was seen, emitted,
measured, stated, or derived. It is accurate, but slightly too clinical and
too passive for the role.

Leading candidate: **signal**.

Why signal fits:

- It can be raw, structured, derived, dense, sparse, noisy, or high-confidence.
- It does not imply truth. It only says that information entered the system.
- It fits event streams and summaries equally well.
- It naturally supports later language like signal strength, signal source,
  signal correlation, signal decay, and signal extraction.
- It leaves room for facts to be stronger epistemic objects layered on top.

Other candidates considered:

- **Observation:** precise, but too passive and too tied to a human/scientific
  observer.
- **Trace:** excellent for provenance, but less natural for intentional
  assertions and derived summaries.
- **Evidence:** strong epistemology, but implies argument or proof too early.
- **Sample:** good for the manifold analogy, but too numerical/statistical.
- **Record:** practical, but too storage-shaped.
- **Datum:** accurate, but sterile and too low-level.

Current decision: use **signal** in design language until a stronger term
appears.

Working distinction:

- **Signal:** an information-bearing unit with provenance.
- **Fact:** a structured assertion derived from, emitted with, or attached to
  one or more signals.
- **Summary:** a derived signal that compresses other signals.
- **View:** an organizing projection over signals and facts.

Avoid treating this as final vocabulary. The useful constraint is that the
atom must be general enough for both natural-language history and non-textual
event streams.

## Core Framing

MIP should not own a single canonical hierarchy. App-native hierarchies,
semantic clusters, timelines, derivation chains, and user-curated context
paths are all projections over the same underlying signals.

The substrate is therefore not a pure tree and not only a DAG. It is a
temporal, attributed knowledge graph with DAG-shaped projections where those
projections are useful.

The clearest split:

- The **knowledge engine** stores signals, facts, provenance, indexes, and
  derived views.
- The **MIP engine** creates level-of-detail summaries and other derived
  signals over that substrate.
- Hyperion's **context engine** builds acyclic context projections from the
  substrate for model input.

This keeps Hyperion focused on curation and linearization while allowing the
knowledge substrate to remain cyclic, temporal, uncertain, and cross-domain.

## Minimal App Contract

Apps should be able to stream information into the system without adopting a
large ontology. The app-side contract should stay close to these verbs:

- **observe:** submit a signal with provenance, time, content or value, and
  optional metadata.
- **assert:** attach a structured fact to a subject.
- **relate:** declare a relationship between subjects.
- **view:** provide an app-native organization, ordering, or membership.
- **checkpoint:** mark ingestion progress.

The engine can then emit derived material back:

- summaries
- correlation candidates
- derived facts
- derived views
- drift or conflict notices
- context candidates

The important property is bidirectionality: apps can publish their native
structure, but the central engine can suggest cross-app structure without
rewriting app truth.

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

## Design Constraints

- Raw signals must remain available even when summaries exist.
- Derived signals must carry provenance back to source signals.
- Apps should not need to predict future global ontology decisions.
- The engine should support dense data that is rolled up or indexed rather
  than summarized as prose.
- Time should be first-class because it is the universal cross-app correlator.
- Human or agent curation should become high-value signal, not hidden UI state.
- A weak correlation should be cheap to record and easy to supersede.
- A strong correlation should be explainable through supporting facts and
  source signals.

## Open Questions

- Is **signal** the right atom name, or should it be reserved for inferred
  importance while the atom keeps a more neutral name?
- Should facts be stored as first-class objects from day one, or begin as
  structured properties on signals and later promote?
- How much property-class normalization should happen at ingestion time versus
  asynchronously?
- What is the smallest query language that can express correlation salience
  without becoming a premature ontology language?
- Where should MIP stop and Hyperion's context engine begin?
