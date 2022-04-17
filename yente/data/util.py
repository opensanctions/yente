from datetime import datetime
from prefixdate.precision import Precision
from typing import List


def iso_datetime(value: str) -> datetime:
    """Parse a second-precision ISO date time string."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def expand_dates(dates: List[str]):
    """Expand a date into less precise versions of itself."""
    expanded = set(dates)
    for date in dates:
        for prec in (Precision.DAY, Precision.MONTH, Precision.YEAR):
            if len(date) > prec.value:
                expanded.add(date[: prec.value])
    return list(expanded)
