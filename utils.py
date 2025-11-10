from collections import defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
import time


@contextmanager
def timed_section(profile: dict[str, float], key: str):
    """
    Context manager to accumulate elapsed time per key.
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        duration = time.perf_counter() - start
        profile[key] = profile.get(key, 0.0) + duration


def summarize_profiles(profiles: Iterable[dict[str, float]]) -> dict[str, float]:
    """
    Aggregate profiling data across all jobs.
    """
    aggregate: dict[str, float] = defaultdict(float)
    for profile in profiles:
        for key, value in profile.items():
            aggregate[key] += value
    return dict(aggregate)


def format_profile_summary(profiles: list[dict[str, float]]) -> str:
    """
    Produce a human-readable summary string for profiling data.
    """
    if not profiles:
        return ""

    summary = summarize_profiles(profiles)
    job_count = len(profiles)

    lines = ["", "Profiling summary (seconds):"]
    for key in sorted(summary, key=summary.get, reverse=True):
        total = summary[key]
        avg = total / job_count if job_count else 0.0
        lines.append(f"{key:>30}: total={total:0.4f}s avg={avg:0.4f}s")
    return "\n".join(lines)
