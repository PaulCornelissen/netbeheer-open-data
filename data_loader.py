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
    path: str = path_from(path_from("data", dso), f"{dso}-{year}.csv")
    return pd.read_csv(path, sep=separator, engine="python", decimal=",", error_bad_lines=False, warn_bad_lines=True)
