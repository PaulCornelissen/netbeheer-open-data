from enum import Enum
from file_utils import path_from
import pandas as pd


class DSO(str, Enum):
    """
    Enum of possible DSO names
    """

    ENEXIS = "enexis"
    LIANDER = "liander"
    STEDIN = "stedin"
    WESTLAND = "westland-infra"


def df_by_src_year(dso: DSO, year: int = 2022, separator: str = "\t") -> pd.DataFrame:
    """
    Get a DataFrame for a given DSO and
    """
    dso_name = dso.value
    path: str = path_from(path_from("data", dso_name), f"{dso_name}-{year}.csv")
    return pd.read_csv(
        path,
        sep=separator,
        engine="python",
        decimal=",",
        on_bad_lines="warn",
        encoding_errors="ignore",
    )
