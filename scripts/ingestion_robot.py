from __future__ import annotations

import os
from typing import Iterable

import pandas as pd
import requests
from dotenv import load_dotenv


KEY_COLUMNS = ["CNPJ_Fundo_Classe", "Data_Referencia", "Versao"]
DATE_COLUMNS = ["Data_Referencia", "Data_Entrega"]
NUMERIC_COLUMNS = [
    "Patrimonio_Liquido",
    "Total_Investido",
    "Cotas_Emitidas",
    "Numero_Cotistas",
    "Rendimento_Distribuido",
    "Rendimento_Cota",
    "Preco_Cota",
]


def _read_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, sep=";", low_memory=False)
    df.columns = [column.strip() for column in df.columns]
    return df


def _coerce_dates(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def _coerce_numeric(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _filter_latest_versions(df: pd.DataFrame) -> pd.DataFrame:
    if "Versao" in df.columns:
        df["Versao"] = pd.to_numeric(df["Versao"], errors="coerce")
    df = df.dropna(subset=["CNPJ_Fundo_Classe", "Data_Referencia"])
    df = df.sort_values("Versao", ascending=False, na_position="last")
    return df.drop_duplicates(subset=["CNPJ_Fundo_Classe", "Data_Referencia"], keep="first")


def _get_first_value(row: pd.Series, columns: Iterable[str]) -> float | None:
    for column in columns:
        if column in row and pd.notna(row[column]):
            return float(row[column])
    return None


def _asset_class_from_row(row: pd.Series) -> str:
    class_text = " ".join(str(value) for value in row.values if isinstance(value, str)).lower()
    if "fiagro" in class_text:
        return "fiagro"
    return "fii"



def _load_env() -> tuple[str, str]:
    load_dotenv()
    ingest_url = os.environ["MONIITOR_INGEST_URL"].rstrip("/")
    ingest_api_key = os.environ["INGEST_API_KEY"]
    return ingest_url, ingest_api_key


def send_to_ingest_function(
    ingest_url: str,
    ingest_api_key: str,
    data_type: str,
    records: list[dict],
) -> None:
    if not records:
        return
    headers = {"x-api-key": ingest_api_key, "Content-Type": "application/json"}
    payload = {"type": data_type, "data": records}
    response = requests.post(ingest_url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()


def _build_current_payloads(df: pd.DataFrame) -> list[dict]:
    latest_dates = df.groupby("CNPJ_Fundo_Classe")["Data_Referencia"].max()
    latest_df = df.join(latest_dates, on="CNPJ_Fundo_Classe", rsuffix="_max")
    latest_df = latest_df[latest_df["Data_Referencia"] == latest_df["Data_Referencia_max"]]

    payloads = []
    for _, row in latest_df.iterrows():
        patrimonio = _get_first_value(row, ["Patrimonio_Liquido"])
        cotas = _get_first_value(row, ["Cotas_Emitidas"])
        vpa = patrimonio / cotas if patrimonio and cotas else None
        num_cotistas = _get_first_value(row, ["Numero_Cotistas"])
        payloads.append(
            {
                "cnpj_fundo_classe": row["CNPJ_Fundo_Classe"],
                "asset_class": _asset_class_from_row(row),
                "data_referencia": row["Data_Referencia"].date().isoformat()
                if pd.notna(row["Data_Referencia"])
                else None,
                "patrimonio_liquido": patrimonio,
                "valor_patrimonial_cota": vpa,
                "num_cotistas": num_cotistas,
            }
        )
    return payloads


def _build_vp_history(df: pd.DataFrame) -> list[dict]:
    records = []
    for _, row in df.iterrows():
        patrimonio = _get_first_value(row, ["Patrimonio_Liquido"])
        cotas = _get_first_value(row, ["Cotas_Emitidas"])
        price = _get_first_value(row, ["Preco_Cota"])
        vpa = patrimonio / cotas if patrimonio and cotas else None
        p_vp = price / vpa if price and vpa else None
        records.append(
            {
                "cnpj_fundo_classe": row["CNPJ_Fundo_Classe"],
                "data_referencia": row["Data_Referencia"].date().isoformat()
                if pd.notna(row["Data_Referencia"])
                else None,
                "patrimonio_liquido": patrimonio,
                "cotas_emitidas": cotas,
                "valor_patrimonial_cota": vpa,
                "p_vp": p_vp,
            }
        )
    return records


def _build_dividend_history(df: pd.DataFrame) -> list[dict]:
    dividend_columns = ["Rendimento_Distribuido", "Rendimento_Cota", "Dividendos_Distribuidos"]
    records = []
    for _, row in df.iterrows():
        dividend = _get_first_value(row, dividend_columns)
        if not dividend:
            continue
        records.append(
            {
                "cnpj_fundo_classe": row["CNPJ_Fundo_Classe"],
                "data_referencia": row["Data_Referencia"].date().isoformat()
                if pd.notna(row["Data_Referencia"])
                else None,
                "dividendo": dividend,
            }
        )
    return records


def process_cvm_files(path_geral: str, path_ativo: str, path_complemento: str) -> None:
    ingest_url, ingest_api_key = _load_env()

    geral = _read_csv(path_geral)
    ativo = _read_csv(path_ativo)
    complemento = _read_csv(path_complemento)

    df = geral.merge(ativo, on=KEY_COLUMNS, how="outer").merge(complemento, on=KEY_COLUMNS, how="outer")
    df = _coerce_dates(df, DATE_COLUMNS)
    df = _coerce_numeric(df, NUMERIC_COLUMNS)
    df = _filter_latest_versions(df)

    current_payloads = _build_current_payloads(df)
    vp_history = _build_vp_history(df)
    dividend_history = _build_dividend_history(df)

    send_to_ingest_function(ingest_url, ingest_api_key, "current", current_payloads)
    send_to_ingest_function(ingest_url, ingest_api_key, "metrics", vp_history)
    send_to_ingest_function(ingest_url, ingest_api_key, "dividends", dividend_history)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process CVM monthly reports CSVs.")
    parser.add_argument("path_geral")
    parser.add_argument("path_ativo")
    parser.add_argument("path_complemento")
    args = parser.parse_args()

    process_cvm_files(args.path_geral, args.path_ativo, args.path_complemento)
