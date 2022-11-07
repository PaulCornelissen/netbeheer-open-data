from typing import List, Tuple
from data_loader import df_by_src_year, DSO
import json
import pandas as pd

AA = "ACTIEVE_AANSLUITINGEN"
PS = "PRODUCTSOORT"
PCV = "POSTCODE_VAN"
PCT = "POSTCODE_TOT"

DATA = {
    "liander": {
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
    },
    "stedin": {
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
    },
    "enexis": {
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
    },
}

COLUMN_MAP = {
    "Aantal Aansluitingen": "AANSLUITINGEN_AANTAL",
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

DROP_MAP_2 = ["AANSLUITINGEN_AANTAL", "FYSIEKE_STATUS_PERC"]


def get_active_connections(df) -> pd.DataFrame:
    return df.apply(lambda row: row.AANSLUITINGEN_AANTAL * row.FYSIEKE_STATUS_PERC / 100, axis=1).round().astype(int)


def map_columns(df, column_map=COLUMN_MAP) -> pd.DataFrame:
    return df.rename(columns=column_map)


def filter_columns(df, columns=DROP_MAP) -> pd.DataFrame:
    return df.drop(columns, axis=1, errors="ignore")


def filter_product_type(df, productsoort="GAS") -> pd.DataFrame:
    return df[(df[PS] == productsoort)]


def set_postal_code_index(df) -> pd.DataFrame:
    return df.set_index([PCV, PCT])


def calculate_active_connections(name: str, year: int, kwargs: dict) -> Tuple[int, pd.DataFrame]:
    df = df_by_src_year(dso=name, year=year, **kwargs)
    df = map_columns(df)
    df = filter_product_type(df, productsoort="GAS")
    df = filter_columns(df)
    df[AA] = get_active_connections(df)
    total = df[AA].sum()
    # Let's try to match different years by postal code so we can detect differences by PC
    df = set_postal_code_index(df)
    df = filter_columns(df, columns=DROP_MAP_2)
    df = map_columns(df, column_map={AA: year})
    print(f"{name} had in {year} {total} active connections")
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


results = {}
datasets = {}
for dso, years in DATA.items():
    results[dso] = {}
    dataframes = []
    for year, kwargs in years.items():
        results[dso][year], df = calculate_active_connections(name=dso, year=year, kwargs=kwargs)
        dataframes.append(df)
    # consolidated_data = consolidate_years(dataframes)
    # consolidated_data = calculate_yearly_diff(consolidated_data)

print(json.dumps(results))
