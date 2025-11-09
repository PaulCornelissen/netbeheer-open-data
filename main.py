from typing import List, Tuple, Dict
import time
from data_loader import df_by_src_year, DSO
import json
import pandas as pd

AA = "ACTIEVE_AANSLUITINGEN"
PS = "PRODUCTSOORT"
PCV = "POSTCODE_VAN"
PCT = "POSTCODE_TOT"
FSP = "FYSIEKE_STATUS_PERC"
AANT = "AANSLUITINGEN_AANTAL"

DATA = {
    DSO.LIANDER: {
        2009: {},
        2010: {"separator": ";"},
        2011: {},
        2012: {},
        2013: {},
        2014: {},
        2015: {},
        2016: {},
        2017: {},
        2018: {},
        2019: {},
        2020: {},
        2021: {},
        2022: {},
        2023: {},
        2024: {"separator": ";"},
        # 2025: {"separator": ";"},
    },
    DSO.STEDIN: {
        2009: {},
        2010: {},
        2011: {},
        2012: {},
        2013: {},
        2014: {},
        2015: {},
        2016: {},
        2017: {},
        2018: {},
        2019: {},
        2020: {},
        2021: {},
        2022: {},
        2023: {},
        2024: {},
        2025: {},
    },
    DSO.ENEXIS: {
        2010: {"separator": ";"},
        2011: {"separator": ";"},
        2012: {"separator": ";"},
        2013: {"separator": ";"},
        2014: {"separator": ";"},
        2015: {"separator": ";"},
        2016: {"separator": ";"},
        2017: {"separator": ";"},
        2018: {"separator": ";"},
        2019: {"separator": ";"},
        2020: {"separator": ";"},
        2021: {"separator": ";"},
        2022: {"separator": ";"},
        2023: {"separator": ";"},
        2024: {"separator": ";"},
        2025: {"separator": ";"},
    },
    DSO.WESTLAND: {
        2011: {},
        2012: {},
        2013: {},
        2014: {},
        2015: {},
        2016: {},
        2017: {},
        2018: {},
        2019: {},
        2020: {},
        2021: {},
        2022: {},
        2023: {},
        2024: {},
    },
}

COLUMN_MAP = {
    "Aantal Aansluitingen": AANT,
    "Aantal aansluitingen": AANT,
    "aantal aansluitingen": AANT,
    "aansluiting_aantal" : AANT,
    "%Leveringsrichting": "LEVERINGSRICHTING_PERC",
    "%Fysieke status": "FYSIEKE_STATUS_PERC",
    "%Soort aansluiting": "SOORT_AANSLUITING_PERC",
    "Soort aansluiting Naam": "SOORT_AANSLUITING",
    "SJV": "SJV_GEMIDDELD",
    "%SJV laag tarief": "SJV_LAAG_TARIEF_PERC",
    "%Slimme Meter": "SLIMME_METER_PERC",
}

DROP_MAP = [
    "MEETVERANTWOORDELIJKE",
    "PRODUCTSOORT",
    "VERBRUIKSSEGMENT",
    "%Defintieve aansl (NRM)",
    "%Soort aansluiting",
    "Soort aansluiting Naam",
    "SJV_GEMIDDELD",
    "SJV_LAAG_TARIEF_PERC",
    "Gemiddeld aantal telwielen",
    "LANDCODE",
    "WOONPLAATS",
    "STRAATNAAM",
    "SOORT_AANSLUITING_PERC",
    "SOORT_AANSLUITING",
    "SLIMME_METER_PERC",
    "LEVERINGSRICHTING_PERC",
    "NETBEHEERDER",
    "NETGEBIED",
]

DROP_MAP_2 = [AANT, FSP]


def get_active_connections(df) -> pd.DataFrame:
    """
    Calculates Active Connections by multiplying number of connections with active percentage.
    Returns DataFrame containing the results.
    """
    required = {AANT, FSP}
    missing = required.difference(df.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise KeyError(
            f"Missing required columns for active connection calculation: {missing_cols}"
        )
    return (
        df[AANT]
        .mul(df[FSP])
        .div(100)
        .round()
        .astype(int)
    )


def map_columns(df, column_map=COLUMN_MAP) -> pd.DataFrame:
    """
    Maps inconsistent column names to those used by Stedin
    """
    def _normalize(name):
        if not isinstance(name, str):
            return name
        return name.strip().strip('"').upper()

    df.columns = [_normalize(col) for col in df.columns]
    normalized_map = {_normalize(src): dest for src, dest in column_map.items()}
    return df.rename(columns=normalized_map)


def filter_columns(df, columns=DROP_MAP) -> pd.DataFrame:
    return df.drop(columns, axis=1, errors="ignore")


def filter_product_type(df, productsoort="GAS") -> pd.DataFrame:
    normalized = df[PS].astype(str).str.strip().str.upper()
    return df[normalized == productsoort]


def set_postal_code_index(df) -> pd.DataFrame:
    return df.set_index([PCV, PCT])


def calculate_active_connections(
    dso: DSO, year: int, kwargs: dict
) -> Tuple[int, pd.DataFrame]:
    tic: float = time.perf_counter()
    df = df_by_src_year(dso=dso, year=year, **kwargs)
    df = map_columns(df)
    df = filter_product_type(df, productsoort="GAS")
    df = filter_columns(df)
    try:
        df[AA] = get_active_connections(df)
    except KeyError as exc:
        raise KeyError(f"{dso.value} {year}: {exc}") from exc
    total = df[AA].sum()
    # Let's try to match different years by postal code so we can detect differences by PC
    df = set_postal_code_index(df)
    df = filter_columns(df, columns=DROP_MAP_2)
    df = map_columns(df, column_map={AA: year})
    toc: float = time.perf_counter()
    print(
        f"{dso.value} had in {year} {total} active connections in {toc - tic:0.4f} seconds"
    )
    return int(total), df


def consolidate_years(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    consolidated = pd.concat(dfs, axis=1).fillna(0)
    return consolidated


def calculate_yearly_diff(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    list.sort(cols)
    for cur, next in zip(cols, cols[1:]):
        df[f"{cur}_DIFF"] = df[next] - df[cur]
    return df


def for_years(results: dict, dso: DSO, years: Dict[int, dict], dataframes):
    for year, kwargs in years.items():
        results[dso][year], df = calculate_active_connections(
            dso=dso, year=year, kwargs=kwargs
        )
        dataframes.append(df)


if __name__ == "__main__":
    results = {}
    datasets = {}
    for dso, years in DATA.items():
        results[dso] = {}
        dataframes = []
        for_years(results, dso, years, dataframes)
        # consolidated_data = consolidate_years(dataframes)
        # consolidated_data = calculate_yearly_diff(consolidated_data)

    print(json.dumps(results))
