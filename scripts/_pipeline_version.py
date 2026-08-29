"""The pipeline's output-contract version, stamped into every stage's metadata.

This is a *declared* version, not a measurement: it identifies the shape and
semantics of the four JSON outputs, so a reader (or the agent) can tell whether
two reports produced a week apart came from the same pipeline. The whole
architecture exists to make report diffs attributable to the fleet changing;
a version that moved underneath the reader would break that, so it is recorded
rather than recalled.

Bump it when the outputs change in a way a downstream consumer must notice --
a new or renamed metadata/finding field, a changed statistic or unit, a new
model or check. It is hand-maintained on purpose, the same category as the
dates in ``references/engine-support-lifecycle.md`` and the bands in the
threshold registry: policy/identity a human owns, not a number a script derives.

It is not a git commit: an aws-samples clone or a release tarball may carry no
``.git`` at all, and a provenance field that reads ``unknown`` half the time is
worse than one that states the contract version honestly. The git commit, when
a reader has one, is the finer-grained provenance; this is the coarse one that
is always available.

Kept as its own module (not a constant duplicated across four stages) so the
stages cannot drift to disagreeing versions. It is imported, never run, so it
is not a pipeline stage and a customer never invokes it.
"""

PIPELINE_VERSION = "1.0.0"
