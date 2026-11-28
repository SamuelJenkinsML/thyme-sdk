"""Production calibration pipeline template.

Declares a streaming Thyme dataset, pipeline, and featureset that join
``decision_outcomes`` to ``decision_records`` and maintain windowed per-band
precision, sample counts, Expected Calibration Error (ECE), and a
miscalibration flag as live features.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from thyme.calibration import calibration_report
from thyme.dataset import dataset, field
from thyme.expr import col
from thyme.featureset import extractor, extractor_inputs, extractor_outputs, feature, featureset
from thyme.pipeline import Backfill, Count, inputs, pipeline

DECISION_RECORDS_DATASET = "decision_records"
DECISION_OUTCOMES_DATASET = "decision_outcomes"


def _resolve_featureset_name(target: Any) -> str:
    if isinstance(target, str):
        name = target.strip()
    else:
        name = getattr(target, "__name__", "").strip()
    if not name:
        raise ValueError("featureset must be a @featureset class or non-empty name")
    return name


def _resolve_prob_key_and_json_path(question: str, positive_option: str | None) -> tuple[str, str]:
    q = question.strip()
    if not q:
        raise ValueError("question must be a non-empty string")
    if positive_option is not None:
        opt = positive_option.strip()
        if not opt:
            raise ValueError("positive_option must be non-empty when provided")
        return f"{q}.{opt}", f"{q}.probabilities.{opt}"
    if "." in q:
        head, opt = q.split(".", 1)
        if not head or not opt:
            raise ValueError(f"invalid dotted question '{question}'; expected 'question.option'")
        return q, f"{head}.probabilities.{opt}"
    return q, f"{q}.noul"


def _band_bounds(bins: int) -> list[tuple[int, float, float]]:
    width = 1.0 / bins
    return [
        (i, round(i * width, 6), 1.0 if i == bins - 1 else round((i + 1) * width, 6))
        for i in range(bins)
    ]


def _build_extractor_source(bins: int, min_samples: int, max_ece: float) -> str:
    """Build self-contained Python source for the calibration featureset extractor.

    Capturing a generated function via ``inspect.getsource`` fails for closures
    defined inside a factory, so we compile the extractor with explicit source
    attached as ``__thyme_source__`` (or compile inside a dedicated namespace).
    """
    in_names = []
    out_names = []
    for i in range(bins):
        in_names.append(f"band_{i}_total")
        in_names.append(f"band_{i}_positive")
        out_names.append(f"band_{i}_precision")
    out_names.extend(["total_outcomes", "positive_rate", "ece", "is_miscalibrated"])
    params = ", ".join(["cls", "ts", *in_names])
    width = 1.0 / bins
    lines = [
        f"def compute_calibration({params}):",
        f"    totals = [{', '.join(f'band_{i}_total or 0' for i in range(bins))}]",
        f"    positives = [{', '.join(f'band_{i}_positive or 0' for i in range(bins))}]",
        "    total_outcomes = int(sum(totals))",
        "    total_positive = int(sum(positives))",
        "    positive_rate = (total_positive / total_outcomes) if total_outcomes > 0 else None",
        "    out = {}",
        "    ece_sum = 0.0",
        f"    for idx in range({bins}):",
        "        cnt = totals[idx]",
        "        pos = positives[idx]",
        "        if cnt > 0:",
        "            prec = pos / cnt",
        f"            midpoint = (idx + 0.5) * {width!r}",
        "            ece_sum += (cnt / total_outcomes) * abs(midpoint - prec)",
        "        else:",
        "            prec = None",
        "        out[f'band_{idx}_precision'] = prec",
        "    ece = ece_sum if total_outcomes > 0 else None",
        "    out['total_outcomes'] = total_outcomes",
        "    out['positive_rate'] = positive_rate",
        "    out['ece'] = ece",
        f"    out['is_miscalibrated'] = bool(total_outcomes >= {min_samples} and ece is not None and ece > {max_ece!r})",
        "    return out",
    ]
    return "\n".join(lines)


@dataclass(frozen=True)
class CalibrationTemplate:
    """Registered dataset, pipeline, and featureset for production calibration."""

    featureset_name: str
    question: str
    json_path: str
    positive_outcome: str
    bins: int
    window: str
    min_samples: int
    max_ece: float
    dataset: type
    featureset: type

    @property
    def band_labels(self) -> list[str]:
        return [f"{lo:.1f}-{hi:.1f}" for _, lo, hi in _band_bounds(self.bins)]

    def evaluate(
        self,
        records: Any,
        outcomes: Any = None,
        *,
        attach_window: str | timedelta | None = "30d",
    ) -> dict[str, dict[str, Any]]:
        """Evaluate this template's calibration features on a records/outcomes fixture.

        Returns a mapping of ``model_version -> feature_dict`` matching the
        companion featureset schema (``band_{i}_total``, ``band_{i}_positive``,
        ``band_{i}_precision``, ``total_outcomes``, ``positive_rate``, ``ece``,
        and ``is_miscalibrated``).
        """
        report = calibration_report(
            records,
            outcomes,
            question=self.question,
            positive_outcome=self.positive_outcome,
            bins=self.bins,
            attach_window=attach_window,
        )
        by_model: dict[str, dict[str, Any]] = {}
        for mv, metrics in report.by_model_version.items():
            row: dict[str, Any] = {}
            for idx, b in enumerate(metrics.bins):
                pos = round(b.empirical_rate * b.count) if b.empirical_rate is not None else 0
                row[f"band_{idx}_total"] = b.count
                row[f"band_{idx}_positive"] = pos
                row[f"band_{idx}_precision"] = b.empirical_rate
            row["total_outcomes"] = metrics.count
            row["positive_rate"] = metrics.positive_rate
            row["brier_score"] = metrics.brier_score
            row["ece"] = metrics.ece
            row["is_miscalibrated"] = bool(
                metrics.count >= self.min_samples
                and metrics.ece is not None
                and metrics.ece > self.max_ece
            )
            by_model[mv] = row
        return by_model


def calibration_pipeline(
    target_featureset: Any,
    *,
    question: str,
    positive_outcome: str,
    positive_option: str | None = None,
    bins: int = 10,
    window: str = "30d",
    min_samples: int = 20,
    max_ece: float = 0.08,
    dataset_name: str | None = None,
    featureset_name: str | None = None,
    version: int = 1,
    backfill: Backfill | None = None,
) -> CalibrationTemplate:
    """Create and register a production calibration dataset, pipeline, and featureset.

    The pipeline streams ``decision_outcomes``, filters to ``target_featureset``,
    temporally joins ``decision_records`` on ``decision_id`` to retrieve
    ``model_version`` and ``judgments``, and aggregates windowed per-band counts
    purely inside the Rust engine via ``json_extract`` predicates.
    """
    if bins < 2 or bins > 20:
        raise ValueError("bins must be between 2 and 20")
    if min_samples < 1:
        raise ValueError("min_samples must be at least 1")
    if not (0.0 < max_ece <= 1.0):
        raise ValueError("max_ece must be in (0.0, 1.0]")
    pos_outcome = positive_outcome.strip()
    if not pos_outcome:
        raise ValueError("positive_outcome must be a non-empty string")

    target_name = _resolve_featureset_name(target_featureset)
    prob_key, json_path = _resolve_prob_key_and_json_path(question, positive_option)
    ds_name = dataset_name or f"{target_name}Calibration"
    fs_name = featureset_name or f"{target_name}CalibrationFeatures"
    bounds = _band_bounds(bins)

    # 1. Build and register the @dataset + @pipeline over system datasets.
    ds_annotations: dict[str, Any] = {"model_version": str}
    ds_namespace: dict[str, Any] = {"model_version": field(key=True)}
    for idx, _, _ in bounds:
        ds_annotations[f"band_{idx}_total"] = int
        ds_annotations[f"band_{idx}_positive"] = int
        ds_namespace[f"band_{idx}_total"] = field()
        ds_namespace[f"band_{idx}_positive"] = field()
    ds_annotations["occurred_at"] = datetime
    ds_namespace["occurred_at"] = field(timestamp=True)
    ds_namespace["__annotations__"] = ds_annotations

    @pipeline(version=version, backfill=backfill)
    @inputs(DECISION_OUTCOMES_DATASET, DECISION_RECORDS_DATASET)
    def compute_calibration_bands(cls, outcomes):
        prob = col("judgments").json_extract(json_path)
        aggs = {}
        for idx, lo, hi in bounds:
            in_band = (prob >= lo) & (prob <= hi) if idx == bins - 1 else (prob >= lo) & (prob < hi)
            aggs[f"band_{idx}_total"] = Count(window=window, where=in_band)
            aggs[f"band_{idx}_positive"] = Count(
                window=window,
                where=in_band & (col("outcome") == pos_outcome),
            )
        return (
            outcomes.filter(
                (col("featureset") == target_name) & ~col("decision_id").is_null()
            )
            .join(
                DECISION_RECORDS_DATASET,
                on="decision_id",
                fields=["model_version", "judgments"],
            )
            .groupby("model_version")
            .aggregate(**aggs)
        )

    ds_namespace["compute_calibration_bands"] = compute_calibration_bands
    cal_dataset_cls = dataset(version=version, index=True)(
        type(ds_name, (), ds_namespace)
    )

    # 2. Build and register the companion @featureset exposing band precision & ECE.
    fs_annotations: dict[str, Any] = {"model_version": str}
    fs_namespace: dict[str, Any] = {"model_version": feature()}
    in_names: list[str] = []
    out_names: list[str] = []

    for idx, lo, hi in bounds:
        total_attr = f"band_{idx}_total"
        pos_attr = f"band_{idx}_positive"
        prec_attr = f"band_{idx}_precision"
        fs_annotations[total_attr] = int
        fs_annotations[pos_attr] = int
        fs_annotations[prec_attr] = float
        fs_namespace[total_attr] = feature(
            ref=getattr(cal_dataset_cls, total_attr),
            default=0,
            description=f"Outcomes in probability band [{lo:.1f}, {hi:.1f}]",
        )
        fs_namespace[pos_attr] = feature(
            ref=getattr(cal_dataset_cls, pos_attr),
            default=0,
            description=f"Positive '{pos_outcome}' outcomes in band [{lo:.1f}, {hi:.1f}]",
        )
        fs_namespace[prec_attr] = feature(
            description=f"Empirical precision in probability band [{lo:.1f}, {hi:.1f}]",
        )
        in_names.extend([total_attr, pos_attr])
        out_names.append(prec_attr)

    fs_annotations["total_outcomes"] = int
    fs_annotations["positive_rate"] = float
    fs_annotations["ece"] = float
    fs_annotations["is_miscalibrated"] = bool
    fs_namespace["total_outcomes"] = feature(
        description="Total attached outcomes across all probability bands",
    )
    fs_namespace["positive_rate"] = feature(
        description=f"Empirical rate of '{pos_outcome}' outcomes",
    )
    fs_namespace["ece"] = feature(
        description="Expected Calibration Error across probability bands",
    )
    fs_namespace["is_miscalibrated"] = feature(
        description=f"True when sample count >= {min_samples} and ECE > {max_ece}",
    )
    out_names.extend(["total_outcomes", "positive_rate", "ece", "is_miscalibrated"])

    ext_source = _build_extractor_source(bins, min_samples, max_ece)
    ext_ns: dict[str, Any] = {}
    exec(ext_source, {}, ext_ns)  # noqa: S102 — template-generated pure arithmetic extractor
    ext_fn = ext_ns["compute_calibration"]
    ext_fn._thyme_source_code = ext_source
    ext_fn = extractor(extractor_inputs(*in_names)(extractor_outputs(*out_names)(ext_fn)))

    fs_namespace["__annotations__"] = fs_annotations
    fs_namespace["compute_calibration"] = ext_fn
    cal_featureset_cls = featureset(type(fs_name, (), fs_namespace))

    return CalibrationTemplate(
        featureset_name=target_name,
        question=prob_key,
        json_path=json_path,
        positive_outcome=pos_outcome,
        bins=bins,
        window=window,
        min_samples=min_samples,
        max_ece=max_ece,
        dataset=cal_dataset_cls,
        featureset=cal_featureset_cls,
    )
