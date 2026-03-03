"""
Laboratório de Dados Públicos - BCB
Funções de coleta de dados, cache e processamento.
Based on working modulo_ifdata.py patterns.
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import date
from config import (
    TCB_OVERRIDE, VALID_SR, MIN_ATIVO_TOTAL, MIN_PL,
    RELATORIO_RESUMO, get_short_name,
)


# ─────────────────────────────────────────────
# CORE FETCH FUNCTIONS
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_valores(anomes: int, tipo: int, relatorio: int) -> pd.DataFrame:
    """Fetch IfDataValores for a given period, type, and report."""
    from bcb.odata import IFDATA
    ifdata = IFDATA()
    ep = ifdata.get_endpoint("IfDataValores")
    try:
        df = ep.get(AnoMes=anomes, TipoInstituicao=tipo, Relatorio=relatorio)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_cadastro(anomes: int) -> pd.DataFrame:
    """Fetch IfDataCadastro for a given period."""
    from bcb.odata import IFDATA
    ifdata = IFDATA()
    ep = ifdata.get_endpoint("IfDataCadastro")
    try:
        df = ep.get(AnoMes=anomes)
        return df if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=7200, show_spinner=False)
def find_latest_quarter(tipo: int = 1, relatorio: int = 1, tipos: list = None) -> int:
    """Try recent quarters until finding one with data for given tipo/relatorio.
    If tipos is provided, try each tipo for each quarter."""
    from bcb.odata import IFDATA
    ifdata = IFDATA()
    ep = ifdata.get_endpoint("IfDataValores")

    today = date.today()
    y, m = today.year, today.month
    candidates = []
    for _ in range(8):
        q_month = ((m - 1) // 3) * 3 + 3
        candidates.append(y * 100 + q_month)
        m -= 3
        if m <= 0:
            m += 12
            y -= 1

    tipos_to_try = tipos if tipos else [tipo]

    for anomes in candidates:
        for t in tipos_to_try:
            try:
                df = ep.get(AnoMes=anomes, TipoInstituicao=t, Relatorio=relatorio)
                if df is not None and not df.empty:
                    return anomes
            except Exception:
                continue

    return candidates[-1]


# ─────────────────────────────────────────────
# INSTITUTION FILTERING & SEGMENTATION
# ─────────────────────────────────────────────

def classify_segment(row) -> str:
    """
    Classify institution: Tcb N1/N2/N4 overrides Sr S1-S4.
    """
    tcb = str(row.get("Tcb", "") or "").strip().upper()
    if tcb in TCB_OVERRIDE:
        return tcb
    sr = str(row.get("Sr", "") or "").strip().upper()
    if sr in VALID_SR:
        return sr
    return "Outros"


def build_institution_table(anomes: int) -> pd.DataFrame:
    """
    Build institution lookup table with segmentation.
    Filters: valid segments + PRUDENCIAL suffix.
    Returns: DataFrame with CodInst, NomeInstituicao, NomeReduzido, Segmento_Calculado
    """
    cad = fetch_cadastro(anomes)
    if cad.empty:
        return pd.DataFrame()

    # Classify segments
    cad["Segmento_Calculado"] = cad.apply(classify_segment, axis=1)

    # Keep only valid segments
    cad = cad[cad["Segmento_Calculado"] != "Outros"].copy()

    # Filter PRUDENCIAL institutions (consolidated view)
    cad = cad[cad["NomeInstituicao"].str.contains("PRUDENCIAL", case=False, na=False)].copy()

    if cad.empty:
        return pd.DataFrame()

    # Clean name: remove " – PRUDENCIAL" suffix for display
    cad["NomeDisplay"] = (
        cad["NomeInstituicao"]
        .str.replace(r"\s*[-–]\s*PRUDENCIAL", "", regex=True)
        .str.strip()
    )

    # Add short names
    cad["NomeReduzido"] = cad["NomeDisplay"].apply(get_short_name)

    cad = cad.drop_duplicates(subset=["CodInst"])

    return cad[["CodInst", "NomeInstituicao", "NomeDisplay", "NomeReduzido", "Segmento_Calculado"]].copy()


# ─────────────────────────────────────────────
# DATA EXTRACTION (Long format: NomeColuna + Saldo)
# ─────────────────────────────────────────────

def extract_variable(
    df: pd.DataFrame,
    conta: str | list,
    institutions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract a variable from IfDataValores (long format).
    Data has columns: CodInst, NomeColuna, Saldo, etc.

    Args:
        df: Raw IfDataValores DataFrame
        conta: NomeColuna value (str) or list of NomeColuna values to sum
        institutions: Institution lookup table

    Returns:
        DataFrame with CodInst, NomeDisplay, NomeReduzido, Segmento_Calculado, Saldo
    """
    if df.empty or institutions.empty:
        return pd.DataFrame()

    valid_codes = set(institutions["CodInst"].tolist())

    if isinstance(conta, list):
        # Sum multiple NomeColuna values per institution
        records = {}
        for _, row in df.iterrows():
            cod = row.get("CodInst")
            if cod not in valid_codes:
                continue
            var_name = str(row.get("NomeColuna", ""))
            if var_name in conta:
                val = pd.to_numeric(row.get("Saldo"), errors="coerce")
                if pd.notna(val):
                    records[cod] = records.get(cod, 0) + val
        sub = pd.DataFrame([
            {"CodInst": cod, "Saldo": val}
            for cod, val in records.items()
        ])
    else:
        # Single NomeColuna
        mask = (df["NomeColuna"] == conta) & (df["CodInst"].isin(valid_codes))
        sub = df[mask][["CodInst", "Saldo"]].copy()
        sub["Saldo"] = pd.to_numeric(sub["Saldo"], errors="coerce")

    if sub.empty:
        return pd.DataFrame()

    # Remove NaN, zero, and infinite values
    sub["Saldo"] = pd.to_numeric(sub["Saldo"], errors="coerce")
    sub = sub.dropna(subset=["Saldo"])
    sub = sub[sub["Saldo"].abs() > 0].copy()
    sub = sub[~sub["Saldo"].isin([float("inf"), float("-inf")])].copy()

    # Merge with institution info
    merged = sub.merge(institutions, on="CodInst", how="inner")
    return merged


# ─────────────────────────────────────────────
# ANNUALIZED DRE EXTRACTION (sum of 4 quarters)
# ─────────────────────────────────────────────

def extract_variable_annualized(
    anomes_list: list,
    relatorio: int,
    conta: str | list,
    institutions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract a DRE variable summing over multiple quarters.
    """
    frames = []
    for anomes in anomes_list:
        df = fetch_valores(anomes, 1, relatorio)
        if not df.empty:
            extracted = extract_variable(df, conta, institutions)
            if not extracted.empty:
                frames.append(extracted[["CodInst", "Saldo"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    summed = combined.groupby("CodInst")["Saldo"].sum().reset_index()

    # Re-merge institution info
    result = summed.merge(institutions, on="CodInst", how="inner")
    return result


# ─────────────────────────────────────────────
# MATERIALITY FILTER
# ─────────────────────────────────────────────

def apply_materiality_filter(
    data: pd.DataFrame,
    resumo_df: pd.DataFrame,
    institutions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Apply materiality filters using Resumo (long format).
    - Ativo Total >= 100M (R$ mil)
    - PL >= 20M (R$ mil)
    - Saldo != 0
    """
    if data.empty:
        return data

    valid_codes = set(institutions["CodInst"].tolist())

    # Extract Ativo Total and PL from Resumo
    ativo_mask = (resumo_df["NomeColuna"] == "Ativo Total") & (resumo_df["CodInst"].isin(valid_codes))
    pl_mask = (resumo_df["NomeColuna"] == "Patrimônio Líquido") & (resumo_df["CodInst"].isin(valid_codes))

    valid_by_ativo = set()
    valid_by_pl = set()

    if ativo_mask.any():
        ativo = resumo_df[ativo_mask][["CodInst", "Saldo"]].copy()
        ativo["Saldo"] = pd.to_numeric(ativo["Saldo"], errors="coerce").fillna(0)
        valid_by_ativo = set(ativo[ativo["Saldo"] >= MIN_ATIVO_TOTAL]["CodInst"])

    if pl_mask.any():
        pl = resumo_df[pl_mask][["CodInst", "Saldo"]].copy()
        pl["Saldo"] = pd.to_numeric(pl["Saldo"], errors="coerce").fillna(0)
        valid_by_pl = set(pl[pl["Saldo"] >= MIN_PL]["CodInst"])

    if not valid_by_ativo and not valid_by_pl:
        return data[data["Saldo"] != 0].copy()

    valid_insts = valid_by_ativo & valid_by_pl

    filtered = data[
        (data["CodInst"].isin(valid_insts)) &
        (data["Saldo"] != 0)
    ].copy()

    return filtered


# ─────────────────────────────────────────────
# QUARTER UTILITIES
# ─────────────────────────────────────────────

def get_last_n_quarters(anomes: int, n: int = 4) -> list:
    """Get last N quarters ending at anomes (inclusive)."""
    year = anomes // 100
    month = anomes % 100
    quarters = []
    for _ in range(n):
        quarters.append(year * 100 + month)
        month -= 3
        if month <= 0:
            month += 12
            year -= 1
    return quarters


def format_anomes(anomes: int) -> str:
    """Format AnoMes to human-readable string."""
    year = anomes // 100
    month = anomes % 100
    month_names = {3: "Mar", 6: "Jun", 9: "Set", 12: "Dez"}
    m = month_names.get(month, f"{month:02d}")
    return f"{m}/{year}"


# ─────────────────────────────────────────────
# CREDIT REPORT EXTRACTION (Grupo + NomeColuna="Total")
# ─────────────────────────────────────────────

def extract_credit_variable(
    df: pd.DataFrame,
    grupo: str,
    institutions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Extract a credit variable from IfDataValores (Relatórios 11, 13).
    These reports use Grupo for the modality and NomeColuna for the breakdown.
    We filter NomeColuna == "Total" to get the total per modality.

    For "Total da Carteira de Pessoa Física" (or PJ), Grupo is the NomeColuna
    value itself — so we filter by NomeColuna matching the grupo name.

    Args:
        df: Raw IfDataValores DataFrame
        grupo: Grupo value to filter (e.g., "Habitação") or total marker
        institutions: Institution lookup table

    Returns:
        DataFrame with CodInst, NomeDisplay, NomeReduzido, Segmento_Calculado, Saldo
    """
    if df.empty or institutions.empty:
        return pd.DataFrame()

    valid_codes = set(institutions["CodInst"].tolist())

    # For "Total da Carteira" entries, they appear as NomeColuna values
    # For individual modalities, filter Grupo and take NomeColuna == "Total"
    if "Total da Carteira" in grupo or "Total Exterior" in grupo:
        mask = (df["NomeColuna"] == grupo) & (df["CodInst"].isin(valid_codes))
    else:
        mask = (
            (df["Grupo"] == grupo) &
            (df["NomeColuna"] == "Total") &
            (df["CodInst"].isin(valid_codes))
        )

    sub = df[mask][["CodInst", "Saldo"]].copy()
    sub["Saldo"] = pd.to_numeric(sub["Saldo"], errors="coerce")

    # Remove NaN, zero, infinite
    sub = sub.dropna(subset=["Saldo"])
    sub = sub[sub["Saldo"].abs() > 0].copy()
    sub = sub[~sub["Saldo"].isin([float("inf"), float("-inf")])].copy()

    if sub.empty:
        return pd.DataFrame()

    # Merge with institution info
    merged = sub.merge(institutions, on="CodInst", how="inner")
    return merged


# ─────────────────────────────────────────────
# SEARCH
# ─────────────────────────────────────────────

def search_banks(institutions: pd.DataFrame, query: str) -> pd.DataFrame:
    """Search banks by name."""
    if institutions.empty or not query:
        return pd.DataFrame()
    mask = (
        institutions["NomeDisplay"].str.contains(query, case=False, na=False) |
        institutions["NomeReduzido"].str.contains(query, case=False, na=False)
    )
    return institutions[mask]
