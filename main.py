from concurrent.futures import ProcessPoolExecutor
import os
import time
from data_loader import df_by_src_year, DSO
import json
import pandas as pd
from utils import timed_section, format_profile_summary

AA = "ACTIEVE_AANSLUITINGEN"
PS = "PRODUCTSOORT"
PCV = "POSTCODE_VAN"
PCT = "POSTCODE_TOT"
FSP = "FYSIEKE_STATUS_PERC"
AANT = "AANSLUITINGEN_AANTAL"
ENABLE_PROFILING = True

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
) -> tuple[int, pd.DataFrame, dict[str, float]]:
    profile: dict[str, float] = {}
    tic: float = time.perf_counter()
    with timed_section(profile, "read_csv"):
        df = df_by_src_year(dso=dso, year=year, **kwargs)
    with timed_section(profile, "map_columns_initial"):
        df = map_columns(df)
    with timed_section(profile, "filter_product_type"):
        df = filter_product_type(df, productsoort="GAS")
    with timed_section(profile, "filter_columns_primary"):
        df = filter_columns(df)
    try:
        with timed_section(profile, "calculate_active_connections"):
            df[AA] = get_active_connections(df)
    except KeyError as exc:
        raise KeyError(f"{dso.value} {year}: {exc}") from exc
    with timed_section(profile, "sum_active_connections"):
        total = df[AA].sum()
    # Let's try to match different years by postal code so we can detect differences by PC
    with timed_section(profile, "set_postal_code_index"):
        df = set_postal_code_index(df)
    with timed_section(profile, "filter_columns_post"):
        df = filter_columns(df, columns=DROP_MAP_2)
    with timed_section(profile, "map_columns_year_label"):
        df = map_columns(df, column_map={AA: year})
    toc: float = time.perf_counter()
    profile["total_job_time"] = toc - tic
    if ENABLE_PROFILING:
        print(
            f"{dso.value} had in {year} {total} active connections in {toc - tic:0.4f} seconds"
        )
    return int(total), df, profile


def consolidate_years(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    consolidated = pd.concat(dfs, axis=1).fillna(0)
    return consolidated


def calculate_yearly_diff(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    list.sort(cols)
    for cur, next in zip(cols, cols[1:]):
        df[f"{cur}_DIFF"] = df[next] - df[cur]
    return df


def _process_job(
    args: tuple[DSO, int, dict]
) -> tuple[DSO, int, int, pd.DataFrame, dict[str, float]]:
    """
    Helper to make calculate_active_connections picklable for ProcessPoolExecutor.
    """
    dso, year, kwargs = args
    total, df, profile = calculate_active_connections(
        dso=dso, year=year, kwargs=kwargs
    )
    return dso, year, total, df, profile


if __name__ == "__main__":
    tic: float = time.perf_counter()
    jobs = [
        (dso, year, kwargs)
        for dso, years in DATA.items()
        for year, kwargs in years.items()
    ]

    results = {dso: {} for dso in DATA}
    datasets = {dso: [] for dso in DATA}
    profiling_data: list[dict[str, float]] = []

    if jobs:
        max_workers = min(len(jobs), os.cpu_count() or 1)
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            for dso, year, total, df, profile in pool.map(_process_job, jobs):
                results[dso][year] = total
                datasets[dso].append((year, df))
                if ENABLE_PROFILING:
                    profiling_data.append(profile)

        datasets = {
            dso: [df for year, df in sorted(entries, key=lambda item: item[0])]
            for dso, entries in datasets.items()
        }
        # Example usage:
        # consolidated_data = consolidate_years(datasets[dso])
        # consolidated_data = calculate_yearly_diff(consolidated_data)

    if ENABLE_PROFILING and profiling_data:
        print(format_profile_summary(profiling_data))

    print(json.dumps(results))
    toc: float = time.perf_counter()
    print(f"Finished processing in {toc - tic:0.4f} seconds")
