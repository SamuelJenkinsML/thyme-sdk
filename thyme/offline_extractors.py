"""Running derived-feature extractors on the offline path (TH-312).

The offline path must return the same features as `POST /features/offline`, so
the calling convention here is a faithful port of `generate_wrapper` and
`strip_decorators` in `crates/query-server/src/executor.rs`. Divergence between
the two would show up as a training set that disagrees with the serving path —
the single worst failure a feature platform can have, because a model trains on
one distribution and serves against another.

## Why row-wise is the default

Query-server calls the wrapper **once per row, with scalar values**. Extractors
are therefore written against scalars, and many guard with `if x is None`. Called
with a Polars Series that guard silently does nothing — `Series is None` is just
`False` — so nulls propagate through the arithmetic instead of being caught:

    scalar, null input : False      <- what /features/offline returns
    column, null input : null       <- what a naive vectorised call returns

A training spine produces nulls constantly (every dormant entity, every row
older than `max_lookback`), so that is not a corner case. Vectorised execution is
therefore **opt-in**, consistent with how Thyme treats Python on the write path.

The cost is real and worth stating: row-wise is one Python call per row, so a
20M-row pull with derived features is tens of minutes in the interpreter. A
featureset whose extractors are Series-safe should pass ``vectorized=True`` and
get one call per batch instead.
"""

from __future__ import annotations

from typing import Any

import polars as pl

#: Extractor kind that carries a Python body. `LOOKUP` extractors are
#: synthesised from `feature(ref=...)` and have no source to run.
KIND_PY_FUNC = "PY_FUNC"


def strip_decorators(source_code: str) -> str:
    """Drop decorator lines above the first `def`.

    A port of `strip_decorators` in `crates/query-server/src/executor.rs`,
    including its paren-balance tracking so a multi-line decorator argument list
    is dropped whole. It shares that function's known limitation: parens inside
    string literals on a decorator line are counted naively.
    """
    past_first_def = False
    paren_depth = 0
    result: list[str] = []

    for line in source_code.splitlines():
        if past_first_def:
            result.append(line)
            continue
        if paren_depth > 0:
            paren_depth += line.count("(") - line.count(")")
            continue
        stripped = line.lstrip()
        if stripped.startswith("@"):
            paren_depth += line.count("(") - line.count(")")
            continue
        if stripped.startswith("def "):
            past_first_def = True
        result.append(line)

    return "\n".join(result)


def build_wrapper(
    source_code: str,
    entry_point: str,
    inputs: list[str],
    outputs: list[str],
) -> str:
    """Build the `_wrapper(inputs)` shim query-server builds.

    Ported from `generate_wrapper`. The details that matter for parity: inputs
    are read with ``.get`` so a missing one arrives as ``None`` rather than
    raising, they are passed positionally in declared order after ``cls`` and
    ``ts`` (both ``None``), and a single output that is not already a dict is
    wrapped in one keyed by the output name.
    """
    lines = [strip_decorators(source_code), "", "def _wrapper(inputs):"]

    arg_vars = []
    for i, name in enumerate(inputs):
        lines.append(f'    _arg_{i} = inputs.get("{name}")')
        arg_vars.append(f"_arg_{i}")

    lines.append(f"    _result = {entry_point}(None, None, {', '.join(arg_vars)})")

    if len(outputs) == 1:
        lines.append("    if not isinstance(_result, dict):")
        lines.append(f'        _result = {{"{outputs[0]}": _result}}')

    lines.append("    return _result")
    return "\n".join(lines)


def _compile(extractor: dict) -> Any:
    """Compile an extractor's wrapper and return the callable."""
    wrapper = build_wrapper(
        extractor["source_code"],
        extractor["name"],
        list(extractor.get("inputs", [])),
        list(extractor.get("outputs", [])),
    )
    namespace: dict[str, Any] = {}
    exec(compile(wrapper, f"<extractor {extractor['name']}>", "exec"), namespace)
    return namespace["_wrapper"]


def runnable_extractors(featureset_meta: dict) -> list[dict]:
    """Extractors with a Python body, in declaration order."""
    return [
        e
        for e in featureset_meta.get("extractors", [])
        if e.get("kind", KIND_PY_FUNC) == KIND_PY_FUNC and e.get("source_code")
    ]


def apply_extractors(
    df: pl.DataFrame,
    featureset_meta: dict,
    *,
    vectorized: bool = False,
) -> pl.DataFrame:
    """Compute derived features over a resolved frame.

    Args:
        df: The as-of joined frame, carrying every stored feature.
        featureset_meta: The featureset's registry metadata.
        vectorized: Call each extractor once per frame with Polars Series
            instead of once per row with scalars. Much faster, and only correct
            when the extractor is Series-safe — see the module docstring.

    Returns:
        The frame with one column added per extractor output. Returned unchanged
        when the featureset has no runnable extractors, so a stored-only pull
        costs nothing.
    """
    extractors = runnable_extractors(featureset_meta)
    if not extractors:
        return df

    for extractor in extractors:
        func = _compile(extractor)
        inputs = list(extractor.get("inputs", []))
        outputs = list(extractor.get("outputs", []))

        if vectorized:
            payload = {name: df[name] for name in inputs if name in df.columns}
            produced = func(payload)
            for name in outputs:
                value = produced.get(name)
                if not isinstance(value, pl.Series):
                    # A scalar return broadcasts, matching what a row-wise call
                    # would have produced for every row.
                    value = pl.Series(name, [value] * df.height)
                df = df.with_columns(value.alias(name))
            continue

        # Row-wise: one call per row, exactly as query-server does it.
        produced_rows = [
            func({name: row.get(name) for name in inputs})
            for row in df.iter_rows(named=True)
        ]
        for name in outputs:
            df = df.with_columns(
                pl.Series(name, [r.get(name) for r in produced_rows])
            )

    return df
