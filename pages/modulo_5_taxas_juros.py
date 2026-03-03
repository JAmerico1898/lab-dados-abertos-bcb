"""
Módulo 5: Taxas de Juros — Operações de Crédito
Taxas de juros praticadas por IFs em modalidades de crédito.
Endpoints: TaxasJurosDiariaPorInicioPeriodo e TaxasJurosMensalPorMes.

Sub-módulos (tabs):
1. Ranking: Top/Bottom 10 por modalidade (dado mais recente)
2. Banco Individual: todas as modalidades + posição
3. Gráficos: scatter de mediana ao longo do tempo
4. Download: dados brutos por modalidade e período
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import date, timedelta
from io import BytesIO

from ui_components import render_module_header


# ─────────────────────────────────────────────
# MODALITIES CONFIG
# ─────────────────────────────────────────────

MONTHLY_MODALITIES = [
    "Financiamento imobiliário com taxas de mercado - Prefixado",
    "Financiamento imobiliário com taxas de mercado - Pós-fixado referenciado em IPCA",
]

DAILY_MODALITIES = [
    "Aquisição de veículos - Prefixado",
    "Capital de giro com prazo até 365 dias - Prefixado",
    "Capital de giro com prazo até 365 dias - Pós-fixado referenciado em juros flutuantes",
    "Capital de giro com prazo superior a 365 dias - Prefixado",
    "Capital de giro com prazo superior a 365 dias - Pós-fixado referenciado em juros flutuantes",
    "Cartão de crédito - rotativo total - Prefixado",
    "Cheque especial - Prefixado",
    "Conta garantida - Prefixado",
    "Conta garantida - Pós-fixado referenciado em juros flutuantes",
    "Crédito pessoal consignado privado - Prefixado",
    "Crédito pessoal não consignado - Prefixado",
    "Desconto de duplicatas - Prefixado",
]

ALL_MODALITIES = DAILY_MODALITIES + MONTHLY_MODALITIES

RANKING_EXCLUDED = {
    "Financiamento imobiliário com taxas de mercado - Prefixado",
    "Financiamento imobiliário com taxas de mercado - Pós-fixado referenciado em IPCA",
    "Capital de giro com prazo até 365 dias - Prefixado",
    "Capital de giro com prazo até 365 dias - Pós-fixado referenciado em juros flutuantes",
    "Capital de giro com prazo superior a 365 dias - Pós-fixado referenciado em juros flutuantes",
    "Conta garantida - Pós-fixado referenciado em juros flutuantes",
}

RANKING_MODALITIES = [m for m in ALL_MODALITIES if m not in RANKING_EXCLUDED]


def short_label(mod: str) -> str:
    return mod.replace("Pós-fixado referenciado em ", "Pós-").replace(" - Prefixado", " - Pré")


# ─────────────────────────────────────────────
# DATA FETCHING
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_daily_modality(modality: str, limit: int = 200000) -> pd.DataFrame:
    from bcb import TaxaJuros
    em = TaxaJuros()
    ep = em.get_endpoint("TaxasJurosDiariaPorInicioPeriodo")
    df = (
        ep.query()
        .filter(ep.Modalidade == modality)
        .orderby(ep.InicioPeriodo.desc())
        .limit(limit)
        .collect()
    )
    if not df.empty and "InicioPeriodo" in df.columns:
        df["InicioPeriodo"] = pd.to_datetime(df["InicioPeriodo"], errors="coerce")
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_monthly_modality(modality: str, limit: int = 200000) -> pd.DataFrame:
    from bcb import TaxaJuros
    em = TaxaJuros()
    ep = em.get_endpoint("TaxasJurosMensalPorMes")
    df = (
        ep.query()
        .filter(ep.Modalidade == modality)
        .orderby(ep.Mes.desc())
        .limit(limit)
        .collect()
    )
    if not df.empty and "Mes" in df.columns:
        df["Mes"] = pd.to_datetime(df["Mes"], errors="coerce")
    return df


def get_latest_data(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty:
        return df
    max_date = df[date_col].max()
    return df[df[date_col] == max_date].copy()


def is_daily(modality: str) -> bool:
    return modality in DAILY_MODALITIES


def get_date_col(modality: str) -> str:
    return "InicioPeriodo" if is_daily(modality) else "Mes"


def _load_all_latest(selected_mods: list, progress_holder=None) -> dict:
    results = {}
    total = len(selected_mods)
    for i, mod in enumerate(selected_mods):
        try:
            if is_daily(mod):
                df = fetch_daily_modality(mod, limit=5000)
                date_col = "InicioPeriodo"
            else:
                df = fetch_monthly_modality(mod, limit=5000)
                date_col = "Mes"
            if not df.empty:
                df_latest = get_latest_data(df, date_col)
                if not df_latest.empty:
                    results[mod] = df_latest
        except Exception:
            pass
        if progress_holder:
            progress_holder.progress((i + 1) / total)
    return results


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


# ─────────────────────────────────────────────
# PLOTLY
# ─────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Space Grotesk, sans-serif", color="#94A3B8"),
    margin=dict(l=20, r=20, t=50, b=20),
    hoverlabel=dict(
        bgcolor="#1A2332",
        bordercolor="rgba(148,163,184,0.2)",
        font=dict(color="#F1F5F9"),
    ),
)

GRID_STYLE = dict(gridcolor="rgba(148,163,184,0.07)", showline=False)


def make_median_chart(df: pd.DataFrame, date_col: str, mod_name: str) -> go.Figure:
    cutoff = pd.Timestamp.now() - pd.DateOffset(years=10)
    df_plot = df[df[date_col] >= cutoff].copy()
    if df_plot.empty:
        df_plot = df.copy()

    df_median = df_plot.groupby(date_col)["TaxaJurosAoAno"].median().reset_index()
    df_median = df_median.sort_values(date_col)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_median[date_col],
        y=df_median["TaxaJurosAoAno"],
        mode="markers",
        marker=dict(color="#22D3EE", size=3, opacity=0.6),
        hovertemplate="%{x|%d/%m/%Y}<br>Mediana: %{y:,.2f}% a.a.<extra></extra>",
        showlegend=False,
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text=f"Mediana das Taxas: {short_label(mod_name)}", font=dict(size=13)),
        height=400,
        xaxis={**GRID_STYLE, "title": "Período"},
        yaxis={**GRID_STYLE, "title": "Taxa (% a.a.)"},
    )
    return fig


# ─────────────────────────────────────────────
# HTML TABLE
# ─────────────────────────────────────────────

def render_ranking_table(df: pd.DataFrame) -> str:
    header = """
    <tr>
        <th style="width:40px; text-align:center;">#</th>
        <th>INSTITUIÇÃO</th>
        <th style="text-align:right;">TAXA (% A.A.)</th>
    </tr>"""

    rows = ""
    for i, (_, row) in enumerate(df.iterrows()):
        rank = i + 1
        name = row.get("InstituicaoFinanceira", "—")
        rate = row.get("TaxaJurosAoAno", float("nan"))
        rate_str = f"{rate:,.2f}" if pd.notna(rate) else "—"
        rows += f"""
        <tr>
            <td class="rank-cell">{rank}</td>
            <td class="bank-name">{name}</td>
            <td class="value-cell">{rate_str}</td>
        </tr>"""

    return f"""<table class="top20-table"><thead>{header}</thead><tbody>{rows}</tbody></table>"""


# ─────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────

def render():
    render_module_header(
        icon="💹",
        title="Taxas de Juros",
        subtitle="Rankings de taxas de juros por modalidade de crédito — Dados do BCB",
    )

    tab_ranking, tab_bank, tab_charts = st.tabs([
        "📊 Ranking", "🏦 Banco Individual", "📈 Gráficos"
    ])

    with tab_ranking:
        _render_ranking()
    with tab_bank:
        _render_bank()
    with tab_charts:
        _render_charts()

    st.markdown("""
    <div class="footer">
        Dados: <a href="https://dadosabertos.bcb.gov.br/" target="_blank">dadosabertos.bcb.gov.br</a>
        · TaxaJuros API · Desenvolvido para fins educacionais — COPPEAD/UFRJ
    </div>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
# TAB: RANKING
# ─────────────────────────────────────────────

def _render_ranking():
    selected = st.multiselect(
        "Selecione as modalidades:",
        options=RANKING_MODALITIES,
        default=RANKING_MODALITIES,
        key="tax_rank_mods",
    )

    if not selected:
        return

    query_btn = st.button("Consultar", key="query_tax_rank", type="primary")

    if not query_btn and "tax_ranking_data" not in st.session_state:
        return

    if query_btn:
        progress = st.progress(0, text="Carregando taxas...")
        data = _load_all_latest(selected, progress)
        progress.empty()
        st.session_state.tax_ranking_data = data

    if "tax_ranking_data" not in st.session_state:
        return

    data = st.session_state.tax_ranking_data

    if not data:
        st.warning("Nenhum dado encontrado.")
        return

    # Reference date
    first_df = next(iter(data.values()))
    date_col = "InicioPeriodo" if "InicioPeriodo" in first_df.columns else "Mes"
    if date_col in first_df.columns:
        ref_date = first_df[date_col].max()
        if pd.notna(ref_date):
            st.markdown(
                f"<div style='font-family:Space Mono,monospace; font-size:12px; color:#64748B; "
                f"margin-bottom:16px;'>"
                f"📅 Data de referência: <strong style='color:#22D3EE;'>"
                f"{ref_date.strftime('%d/%m/%Y') if hasattr(ref_date, 'strftime') else ref_date}"
                f"</strong></div>",
                unsafe_allow_html=True,
            )

    for mod in selected:
        if mod not in data:
            continue

        df_latest = data[mod]
        df_latest = df_latest[df_latest["TaxaJurosAoAno"] > 0]
        df_top = df_latest.sort_values("TaxaJurosAoAno", ascending=False).head(10)
        df_bottom = df_latest.sort_values("TaxaJurosAoAno", ascending=True).head(10)
        total_banks = len(df_latest)

        st.markdown(
            f"<div style='font-size:15px; font-weight:600; margin:24px 0 8px; color:#f1f5f9;'>"
            f"{short_label(mod)} "
            f"<span style='font-size:11px; color:#64748B;'>({total_banks} IFs)</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

        col_left, col_right = st.columns(2, gap="medium")

        with col_left:
            st.markdown(
                "<div style='font-size:13px; font-weight:600; color:#FB7185; margin-bottom:6px;'>▲ Maiores Taxas</div>",
                unsafe_allow_html=True,
            )
            st.markdown(render_ranking_table(df_top.reset_index(drop=True)), unsafe_allow_html=True)

        with col_right:
            st.markdown(
                "<div style='font-size:13px; font-weight:600; color:#34D399; margin-bottom:6px;'>▼ Menores Taxas</div>",
                unsafe_allow_html=True,
            )
            st.markdown(render_ranking_table(df_bottom.reset_index(drop=True)), unsafe_allow_html=True)

        st.markdown("<hr style='border-color:rgba(148,163,184,0.06);margin:8px 0;'>", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# TAB: BANCO INDIVIDUAL
# ─────────────────────────────────────────────

def _render_bank():
    query_btn = st.button("Consultar", key="query_tax_bank", type="primary")

    if query_btn:
        progress = st.progress(0, text="Carregando dados...")
        data = _load_all_latest(ALL_MODALITIES, progress)
        progress.empty()
        st.session_state.tax_bank_data = data

    if "tax_bank_data" not in st.session_state:
        if "tax_ranking_data" in st.session_state:
            st.session_state.tax_bank_data = st.session_state.tax_ranking_data
        else:
            st.info("👆 Clique em Consultar para carregar os dados.")
            return

    data = st.session_state.tax_bank_data

    all_banks = set()
    for df in data.values():
        if "InstituicaoFinanceira" in df.columns:
            all_banks.update(df["InstituicaoFinanceira"].dropna().unique())

    selected_bank = st.selectbox("Selecione o banco:", options=sorted(all_banks), key="tax_bank_select")

    if not selected_bank:
        return

    st.markdown(
        f"<div style='font-size:20px; font-weight:700; margin:16px 0 8px; color:#f1f5f9;'>"
        f"🏦 {selected_bank}</div>",
        unsafe_allow_html=True,
    )

    header = """
    <tr>
        <th>MODALIDADE</th>
        <th style="text-align:right;">TAXA (% A.A.)</th>
        <th style="text-align:center;">POSIÇÃO</th>
    </tr>"""

    rows_html = ""
    for mod in ALL_MODALITIES:
        if mod not in data:
            continue

        df_mod = data[mod]
        bank_row = df_mod[df_mod["InstituicaoFinanceira"] == selected_bank]
        if bank_row.empty:
            continue

        rate = bank_row["TaxaJurosAoAno"].iloc[0]
        rate_str = f"{rate:,.2f}" if pd.notna(rate) else "—"

        if pd.notna(rate):
            n = len(df_mod)
            rank = int((df_mod["TaxaJurosAoAno"] < rate).sum() + 1)
            pos_str = f"{rank}º de {n}"
        else:
            pos_str = "—"

        rows_html += f"""
        <tr>
            <td class="bank-name" style="font-size:12px;">{short_label(mod)}</td>
            <td class="value-cell">{rate_str}</td>
            <td class="pct-cell" style="text-align:center;">{pos_str}</td>
        </tr>"""

    if rows_html:
        st.markdown(
            f'<table class="top20-table"><thead>{header}</thead><tbody>{rows_html}</tbody></table>',
            unsafe_allow_html=True,
        )
    else:
        st.info(f"{selected_bank}: dados indisponíveis.")


# ─────────────────────────────────────────────
# TAB: GRÁFICOS
# ─────────────────────────────────────────────

def _render_charts():
    selected_mod = st.selectbox("Selecione a modalidade:", options=ALL_MODALITIES, key="tax_chart_mod")

    if not selected_mod:
        return

    query_btn = st.button("Consultar", key="query_tax_chart", type="primary")

    if query_btn:
        with st.spinner("Carregando dados..."):
            try:
                if is_daily(selected_mod):
                    df = fetch_daily_modality(selected_mod, limit=200000)
                else:
                    df = fetch_monthly_modality(selected_mod, limit=200000)
                st.session_state.tax_chart_data = (selected_mod, df)
            except Exception as e:
                st.error("Erro na consulta.")
                st.code(str(e))
                return

    if "tax_chart_data" not in st.session_state:
        return

    mod_name, df = st.session_state.tax_chart_data
    date_col = get_date_col(mod_name)

    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return

    fig = make_median_chart(df, date_col, mod_name)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.caption(f"📊 {len(df):,} observações")


# ─────────────────────────────────────────────
# TAB: DOWNLOAD
# ─────────────────────────────────────────────

def _render_download():
    st.markdown('<div class="section-title">Download de Dados</div>', unsafe_allow_html=True)
    st.caption("Baixe dados brutos de taxas de juros por modalidade e período.")

    selected_mods = st.multiselect(
        "Modalidades:",
        options=ALL_MODALITIES,
        default=ALL_MODALITIES[:3],
        key="tax_dl_mods",
    )

    col_start, col_end = st.columns(2)
    with col_start:
        dl_start = st.date_input(
            "Data inicial:",
            value=date(2020, 1, 1),
            min_value=date(2000, 1, 1),
            max_value=date.today(),
            format="DD/MM/YYYY",
            key="tax_dl_start",
        )
    with col_end:
        dl_end = st.date_input(
            "Data final:",
            value=date.today(),
            min_value=dl_start,
            max_value=date.today(),
            format="DD/MM/YYYY",
            key="tax_dl_end",
        )

    if not selected_mods:
        return

    dl_btn = st.button("Baixar Dados", key="tax_dl_btn", type="primary")

    if not dl_btn:
        return

    progress = st.progress(0, text="Baixando dados...")
    all_frames = []
    total = len(selected_mods)

    for i, mod in enumerate(selected_mods):
        try:
            if is_daily(mod):
                df = fetch_daily_modality(mod, limit=100000)
                date_col = "InicioPeriodo"
            else:
                df = fetch_monthly_modality(mod, limit=100000)
                date_col = "Mes"

            if not df.empty:
                mask = (df[date_col] >= pd.Timestamp(dl_start)) & (df[date_col] <= pd.Timestamp(dl_end))
                df_filtered = df[mask].copy()
                if not df_filtered.empty:
                    all_frames.append(df_filtered)
        except Exception:
            pass

        progress.progress((i + 1) / total, text=f"Baixando {mod[:40]}...")

    progress.empty()

    if not all_frames:
        st.warning("Nenhum dado encontrado.")
        return

    df_all = pd.concat(all_frames, ignore_index=True)
    st.success(f"✅ {len(df_all):,} registros")

    dl1, dl2, _ = st.columns([1, 1, 4])
    with dl1:
        st.download_button(
            "📥 CSV",
            data=to_csv_bytes(df_all),
            file_name=f"taxas_{dl_start}_{dl_end}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl2:
        st.download_button(
            "📥 XLSX",
            data=to_excel_bytes(df_all),
            file_name=f"taxas_{dl_start}_{dl_end}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.dataframe(df_all, use_container_width=True, hide_index=True, height=400)
