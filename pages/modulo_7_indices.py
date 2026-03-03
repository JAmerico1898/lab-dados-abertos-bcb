"""
Módulo 7: Índices Financeiros
Barras horizontais comparando instituições financeiras por índices calculados.
Índices derivados de Resumo, Ativo, Passivo e DRE.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

from config import (
    SEGMENT_COLORS, ALL_SEGMENTS, DEFAULT_SEGMENTS,
    RELATORIO_RESUMO, RELATORIO_DRE,
    format_brl,
)
from data_utils import (
    fetch_valores, build_institution_table,
    apply_materiality_filter,
    find_latest_quarter, format_anomes, get_last_n_quarters,
)
from ui_components import render_module_header


# ─────────────────────────────────────────────
# RELATÓRIOS
# ─────────────────────────────────────────────
REL_RESUMO = 1
REL_ATIVO = 2
REL_PASSIVO = 3
REL_DRE = 4


# ─────────────────────────────────────────────
# INDEX DEFINITIONS
# ─────────────────────────────────────────────

INDICE_CATEGORIAS = {
    "Ativos": ["op_credito_pct_ativos", "provisoes_pct_carteira"],
    "Capital": ["basileia", "alavancagem", "pl_ajustado"],
    "Resultado": ["result_interm_pct_carteira", "desp_capt_pct_captacoes", "roi", "eficiencia"],
}

INDICES = {
    "op_credito_pct_ativos": {
        "label": "Op. Crédito (% Ativos)",
        "icon": "📊",
        "format": "pct",
        "description": "Carteira de Crédito / Ativo Total",
        "default_desc": True,
    },
    "provisoes_pct_carteira": {
        "label": "Provisões (% Carteira)",
        "icon": "⚠️",
        "format": "pct",
        "description": "Perda Esperada / Operações de Crédito",
        "default_desc": False,
    },
    "basileia": {
        "label": "Índice de Basileia",
        "icon": "🏛️",
        "format": "pct_direct",
        "description": "Índice de Basileia (direto do Resumo)",
        "default_desc": True,
    },
    "alavancagem": {
        "label": "Alavancagem (PL/Ativos)",
        "icon": "⚖️",
        "format": "pct",
        "description": "Patrimônio Líquido / Ativo Total",
        "default_desc": True,
    },
    "pl_ajustado": {
        "label": "Patrimônio Líquido",
        "icon": "🛡️",
        "format": "brl",
        "description": "Patrimônio Líquido Ajustado",
        "default_desc": True,
    },
    "result_interm_pct_carteira": {
        "label": "Result. Intermediação (% Carteira)",
        "icon": "🔄",
        "format": "pct",
        "description": "Resultado de Intermediação / Carteira de Crédito",
        "default_desc": True,
    },
    "desp_capt_pct_captacoes": {
        "label": "Despesa Captação (% Captações)",
        "icon": "💸",
        "format": "pct",
        "description": "Despesas de Captações / Captações",
        "default_desc": False,
    },
    "roi": {
        "label": "ROI (Lucro / PL)",
        "icon": "📈",
        "format": "pct",
        "description": "Lucro Líquido / Patrimônio Líquido",
        "default_desc": True,
    },
    "eficiencia": {
        "label": "Índice de Eficiência",
        "icon": "⚡",
        "format": "pct",
        "description": "Receita de Serviços / Despesas Operacionais",
        "default_desc": True,
    },
}


# ─────────────────────────────────────────────
# VARIABLE EXTRACTION HELPER
# ─────────────────────────────────────────────

def _extract_series(df: pd.DataFrame, nome_coluna: str, valid_codes: set) -> pd.Series:
    """Extract a NomeColuna as a Series indexed by CodInst."""
    # Handle name changes between quarters by partial matching
    # First try exact match
    mask = (df["NomeColuna"] == nome_coluna) & (df["CodInst"].isin(valid_codes))
    sub = df[mask][["CodInst", "Saldo"]].copy()

    if sub.empty:
        # Try partial match (before the \n)
        prefix = nome_coluna.split("\n")[0].strip()
        mask = df["NomeColuna"].str.startswith(prefix, na=False) & df["CodInst"].isin(valid_codes)
        sub = df[mask][["CodInst", "Saldo"]].copy()

    sub["Saldo"] = pd.to_numeric(sub["Saldo"], errors="coerce")
    sub = sub.dropna(subset=["Saldo"])
    return sub.set_index("CodInst")["Saldo"]


def _extract_sum(df: pd.DataFrame, nomes: list, valid_codes: set) -> pd.Series:
    """Sum multiple NomeColuna values per CodInst."""
    total = pd.Series(dtype=float)
    for nome in nomes:
        s = _extract_series(df, nome, valid_codes)
        total = total.add(s, fill_value=0)
    return total


def _annualize_series(quarters: list, relatorio: int, nome_coluna: str, valid_codes: set) -> pd.Series:
    """Sum a DRE variable across 4 quarters."""
    total = pd.Series(dtype=float)
    for q in quarters:
        df = fetch_valores(q, tipo=1, relatorio=relatorio)
        if df.empty:
            continue
        s = _extract_series(df, nome_coluna, valid_codes)
        total = total.add(s, fill_value=0)
    return total


def _annualize_sum(quarters: list, relatorio: int, nomes: list, valid_codes: set) -> pd.Series:
    """Sum multiple NomeColuna across 4 quarters."""
    total = pd.Series(dtype=float)
    for q in quarters:
        df = fetch_valores(q, tipo=1, relatorio=relatorio)
        if df.empty:
            continue
        s = _extract_sum(df, nomes, valid_codes)
        total = total.add(s, fill_value=0)
    return total


# ─────────────────────────────────────────────
# COMPUTE ALL INDICES
# ─────────────────────────────────────────────

def compute_indices(anomes: int, institutions: pd.DataFrame) -> dict:
    """
    Compute all 9 indices for all institutions.
    Returns dict: {index_key: pd.Series indexed by CodInst}
    """
    valid_codes = set(institutions["CodInst"].tolist())
    quarters = get_last_n_quarters(anomes, n=4)

    # Load base reports (latest quarter)
    resumo = fetch_valores(anomes, tipo=1, relatorio=REL_RESUMO)
    ativo = fetch_valores(anomes, tipo=1, relatorio=REL_ATIVO)
    passivo = fetch_valores(anomes, tipo=1, relatorio=REL_PASSIVO)

    # Resumo variables (latest quarter)
    ativo_total = _extract_series(resumo, "Ativo Total", valid_codes)
    carteira_credito = _extract_series(resumo, "Carteira de Crédito", valid_codes)
    pl = _extract_series(resumo, "Patrimônio Líquido", valid_codes)
    basileia_raw = _extract_series(resumo, "Índice de Basileia", valid_codes)
    captacoes_resumo = _extract_series(resumo, "Captações", valid_codes)

    # Lucro Líquido annualized (sum 4 quarters from Resumo)
    lucro_anual = _annualize_series(quarters, REL_RESUMO, "Lucro Líquido", valid_codes)

    # Ativo variables
    perda_e2 = _extract_series(ativo, "Perda Esperada \n(e2)", valid_codes)
    perda_g2 = _extract_series(ativo, "Perda Esperada \n(g2)", valid_codes)
    op_cred_e = _extract_series(ativo, "Operações de Crédito \n(e)", valid_codes)
    op_cred_g = _extract_series(ativo, "Outras Operações com Características de Concessão de Crédito \n(g)", valid_codes)

    # Passivo variables
    captacoes_passivo = _extract_series(passivo, "Captações \n(e) = (a) + (b) + (c) + (d)", valid_codes)

    # DRE variables (annualized)
    result_interm = _annualize_series(quarters, REL_DRE, "Resultado de Intermediação Financeira \n(k) = (a) + (b) + (c) + (d) + (e) + (f) + (g) + (h) + (i) + (j)", valid_codes)
    desp_capt_dre = _annualize_series(quarters, REL_DRE, "Despesas de Captações \n(g)", valid_codes)
    rendas_tarifas = _annualize_series(quarters, REL_DRE, "Rendas de Tarifas Bancárias \n(m)", valid_codes)
    rendas_servicos = _annualize_series(quarters, REL_DRE, "Outras Rendas de Prestação de Serviços \n(n)", valid_codes)
    desp_pessoal = _annualize_series(quarters, REL_DRE, "Despesas de Pessoal \n(o)", valid_codes)
    desp_admin = _annualize_series(quarters, REL_DRE, "Despesas Administrativas \n(p)", valid_codes)

    # ─── Compute indices ───
    results = {}

    # 1. Op Crédito (% Ativos)
    results["op_credito_pct_ativos"] = (carteira_credito / ativo_total * 100).dropna()

    # 2. Provisões (% Carteira)
    perda_total = perda_e2.add(perda_g2, fill_value=0)
    op_total = op_cred_e.add(op_cred_g, fill_value=0)
    results["provisoes_pct_carteira"] = (perda_total.abs() / op_total.abs() * 100).replace([np.inf, -np.inf], np.nan).dropna()

    # 3. Basileia (direto — API returns as fraction, e.g. 0.25 = 25%)
    results["basileia"] = (basileia_raw * 100).dropna()

    # 4. Alavancagem (PL / Ativos)
    results["alavancagem"] = (pl / ativo_total * 100).dropna()

    # 5. PL Ajustado (valor absoluto)
    results["pl_ajustado"] = pl.dropna()

    # 6. Resultado Intermediação (% Carteira)
    results["result_interm_pct_carteira"] = (result_interm / carteira_credito * 100).replace([np.inf, -np.inf], np.nan).dropna()

    # 7. Despesa Captação (% Captações)
    captacoes = captacoes_passivo if not captacoes_passivo.empty else captacoes_resumo
    results["desp_capt_pct_captacoes"] = (desp_capt_dre.abs() / captacoes.abs() * 100).replace([np.inf, -np.inf], np.nan).dropna()

    # 8. ROI (Lucro Anualizado / PL)
    results["roi"] = (lucro_anual / pl * 100).replace([np.inf, -np.inf], np.nan).dropna()

    # 9. Eficiência (Receita Serviços / Despesas Operacionais)
    receita_serv = rendas_tarifas.add(rendas_servicos, fill_value=0)
    desp_oper = desp_pessoal.add(desp_admin, fill_value=0)
    results["eficiencia"] = (receita_serv / desp_oper.abs() * 100).replace([np.inf, -np.inf], np.nan).dropna()

    return results


# ─────────────────────────────────────────────
# BAR CHART
# ─────────────────────────────────────────────

def make_bar_chart(df_plot: pd.DataFrame, index_info: dict, n_bars: int = 20) -> go.Figure:
    """Create horizontal bar chart for an index."""
    fmt = index_info["format"]

    fig = go.Figure()

    colors = [SEGMENT_COLORS.get(seg, "#718096") for seg in df_plot["Segmento_Calculado"]]

    if fmt == "brl":
        text_vals = [format_brl(v) for v in df_plot["Valor"]]
    elif fmt in ("pct", "pct_direct"):
        text_vals = [f"{v:.1f}%" for v in df_plot["Valor"]]
    else:
        text_vals = [f"{v:.2f}" for v in df_plot["Valor"]]

    fig.add_trace(go.Bar(
        y=df_plot["NomeReduzido"],
        x=df_plot["Valor"],
        orientation="h",
        marker=dict(color=colors, cornerradius=4),
        text=text_vals,
        textposition="outside",
        textfont=dict(size=11, family="Space Mono", color="#94a3b8"),
        cliponaxis=False,
        hovertemplate=(
            "<b>%{y}</b><br>"
            + ("Valor: %{text}" if fmt == "brl" else "Valor: %{x:.2f}%")
            + "<extra></extra>"
        ),
    ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Space Grotesk", color="#f1f5f9"),
        margin=dict(l=10, r=150, t=10, b=10),
        height=max(400, n_bars * 32),
        yaxis=dict(
            gridcolor="rgba(148,163,184,0.05)",
            tickfont=dict(size=12),
        ),
        xaxis=dict(
            gridcolor="rgba(148,163,184,0.07)",
            showline=False,
        ),
        showlegend=False,
    )

    return fig


# ─────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────

def render():
    """Main render function for Module 7."""

    # Session state
    if "mod7_selected_idx" not in st.session_state:
        st.session_state.mod7_selected_idx = None
    if "mod7_segments" not in st.session_state:
        st.session_state.mod7_segments = DEFAULT_SEGMENTS.copy()

    # Header
    render_module_header(
        icon="📐",
        title="Índices Financeiros",
        subtitle="Barras horizontais comparando instituições por indicadores de Ativos, Capital e Resultado",
    )

    # ─────────────────────────────────────────
    # INDEX SELECTOR (by category)
    # ─────────────────────────────────────────
    for cat_name, idx_keys in INDICE_CATEGORIAS.items():
        st.markdown(f"###### {cat_name}:")
        cols = st.columns(len(idx_keys))
        for i, idx_key in enumerate(idx_keys):
            idx_info = INDICES[idx_key]
            is_selected = st.session_state.mod7_selected_idx == idx_key
            with cols[i]:
                if st.button(
                    f"{idx_info['icon']}  {idx_info['label']}",
                    key=f"sel7_{idx_key}",
                    use_container_width=True,
                    type="primary" if is_selected else "secondary",
                ):
                    st.session_state.mod7_selected_idx = idx_key
                    st.rerun()

    selected_idx = st.session_state.mod7_selected_idx
    selected_info = INDICES.get(selected_idx) if selected_idx else None

    # Segment filter
    st.markdown("---")
    st.markdown("##### Filtro de Segmentação:")

    seg_cols = st.columns(len(ALL_SEGMENTS))
    new_segments = []
    for i, seg in enumerate(ALL_SEGMENTS):
        with seg_cols[i]:
            is_on = seg in st.session_state.mod7_segments
            if st.checkbox(seg, value=is_on, key=f"seg7_{seg}"):
                new_segments.append(seg)

    if not new_segments:
        new_segments = ALL_SEGMENTS.copy()
    st.session_state.mod7_segments = new_segments

    # Guard
    if selected_info is None:
        st.info("👆 Selecione um índice acima para visualizar os dados.")
        return

    # ─────────────────────────────────────────
    # DATA LOADING
    # ─────────────────────────────────────────
    st.markdown("---")

    anomes = find_latest_quarter()

    with st.spinner(f"Calculando índices ({format_anomes(anomes)})..."):
        institutions = build_institution_table(anomes)
        if institutions.empty:
            st.error("❌ Não foi possível carregar os dados de cadastro.")
            return

        # Materiality filter
        resumo_df = fetch_valores(anomes, tipo=1, relatorio=RELATORIO_RESUMO)
        institutions_filtered = apply_materiality_filter(
            institutions.assign(Saldo=1),  # dummy saldo to pass through
            resumo_df, institutions
        )
        if institutions_filtered.empty:
            st.error("❌ Nenhuma instituição passou no filtro de materialidade.")
            return

        # Use filtered institution codes
        inst_filtered = institutions[institutions["CodInst"].isin(institutions_filtered["CodInst"])]

        # Compute all indices
        all_indices = compute_indices(anomes, inst_filtered)

    if selected_idx not in all_indices or all_indices[selected_idx].empty:
        st.warning(f"⚠️ Nenhum dado encontrado para '{selected_info['label']}'.")
        return

    # Build display DataFrame
    idx_series = all_indices[selected_idx]
    df_display = inst_filtered[["CodInst", "NomeReduzido", "NomeDisplay", "Segmento_Calculado"]].copy()
    df_display = df_display[df_display["CodInst"].isin(idx_series.index)]
    df_display["Valor"] = df_display["CodInst"].map(idx_series)
    df_display = df_display.dropna(subset=["Valor"])

    # Filter extreme outliers (beyond 3 std)
    if selected_info["format"] == "pct":
        mean_val = df_display["Valor"].mean()
        std_val = df_display["Valor"].std()
        if std_val > 0:
            df_display = df_display[
                (df_display["Valor"] >= mean_val - 3 * std_val) &
                (df_display["Valor"] <= mean_val + 3 * std_val)
            ]

    # Segment filter
    df_display = df_display[df_display["Segmento_Calculado"].isin(st.session_state.mod7_segments)].copy()

    if df_display.empty:
        st.warning("⚠️ Nenhuma instituição atende aos critérios.")
        return

    # Sort order
    default_desc = selected_info.get("default_desc", True)

    sort_col, _ = st.columns([3, 5])
    with sort_col:
        reverse = st.checkbox(
            "🔄 Inverter ordenação",
            value=False,
            key=f"mod7_reverse_{selected_idx}",
        )

    # default_desc=True means largest first (descending)
    # reverse flips it
    sort_descending = default_desc if not reverse else not default_desc
    df_sorted = df_display.sort_values("Valor", ascending=not sort_descending)

    # ─────────────────────────────────────────
    # METRICS
    # ─────────────────────────────────────────
    n_insts = len(df_sorted)
    median_val = df_sorted["Valor"].median()
    mean_val = df_sorted["Valor"].mean()

    fmt = selected_info["format"]
    if fmt == "brl":
        fmt_median = format_brl(median_val)
        fmt_mean = format_brl(mean_val)
    else:
        fmt_median = f"{median_val:.1f}%"
        fmt_mean = f"{mean_val:.1f}%"

    mcol1, mcol2, mcol3, mcol4 = st.columns(4)
    with mcol1:
        st.metric("Mediana", fmt_median)
    with mcol2:
        st.metric("Média", fmt_mean)
    with mcol3:
        st.metric("Nº de Instituições", f"{n_insts}")
    with mcol4:
        st.metric("Período", format_anomes(anomes))

    # ─────────────────────────────────────────
    # BAR CHART — TOP 20
    # ─────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"### {selected_info['icon']} {selected_info['label']}")
    st.caption(selected_info["description"])

    n_show = min(20, len(df_sorted))
    df_top = df_sorted.head(n_show)
    # Plotly horizontal bars render bottom-to-top
    # Reverse data so rank #1 appears at the top of the chart
    df_chart = df_top.iloc[::-1].reset_index(drop=True)

    fig = make_bar_chart(df_chart, selected_info, n_show)
    st.plotly_chart(fig, use_container_width=True)

    # ─────────────────────────────────────────
    # FULL TABLE
    # ─────────────────────────────────────────
    st.markdown("#### 📋 Ranking Completo")

    seg_colors_table = {
        "S1": "#22d3ee", "S2": "#34d399", "S3": "#fbbf24", "S4": "#a78bfa",
        "N1": "#fb7185", "N2": "#f97316", "N4": "#38bdf8",
    }

    header_html = f"""
    <tr>
        <th style="width:45px; text-align:center;">#</th>
        <th>INSTITUIÇÃO</th>
        <th style="text-align:center; width:100px;">SEGMENTO</th>
        <th style="text-align:right;">{selected_info['label'].upper()}</th>
    </tr>"""

    rows_html = ""
    for i, (_, row) in enumerate(df_sorted.head(20).iterrows()):
        rank = i + 1
        seg = row["Segmento_Calculado"]
        seg_color = seg_colors_table.get(seg, "#718096")

        if fmt == "brl":
            val_str = format_brl(row["Valor"])
        else:
            val_str = f"{row['Valor']:.2f}%"

        medal = ""
        if rank == 1:
            medal = "🥇"
        elif rank == 2:
            medal = "🥈"
        elif rank == 3:
            medal = "🥉"

        rows_html += f"""
        <tr>
            <td class="rank-cell">{medal if medal else rank}</td>
            <td>
                <div class="bank-name">{row['NomeReduzido']}</div>
                <div class="bank-full">{row['NomeDisplay']}</div>
            </td>
            <td style="text-align:center;">
                <span class="seg-pill" style="background:{seg_color}18; color:{seg_color};">{seg}</span>
            </td>
            <td class="value-cell">{val_str}</td>
        </tr>"""

    st.markdown(
        f"""<table class="top20-table"><thead>{header_html}</thead><tbody>{rows_html}</tbody></table>""",
        unsafe_allow_html=True,
    )

    # Search
    st.markdown("---")
    search_query = st.text_input("🔍 Buscar instituição", placeholder="Digite o nome do banco...", key="mod7_search")

    if search_query:
        mask = df_sorted["NomeDisplay"].str.contains(search_query, case=False, na=False)
        search_results = df_sorted[mask].copy()
        if not search_results.empty:
            search_results["Rank"] = df_sorted["Valor"].rank(ascending=not sort_descending, method="min").astype(int)
            st.info(f"🔍 {len(search_results)} resultado(s)")
            show_df = search_results[["NomeReduzido", "Segmento_Calculado", "Valor", "Rank"]].copy()
            if fmt == "brl":
                show_df["Valor"] = show_df["Valor"].apply(format_brl)
            else:
                show_df["Valor"] = show_df["Valor"].apply(lambda v: f"{v:.2f}%")
            st.dataframe(
                show_df.reset_index(drop=True).rename(columns={
                    "NomeReduzido": "Instituição",
                    "Segmento_Calculado": "Segmento",
                    "Valor": selected_info["label"],
                    "Rank": "Posição",
                }),
                use_container_width=True,
            )
        else:
            st.warning(f"Nenhuma instituição encontrada para '{search_query}'.")

    # Footer
    st.markdown("---")
    quarters = get_last_n_quarters(anomes, n=4)
    period_label = f"{format_anomes(quarters[-1])} a {format_anomes(quarters[0])}"
    st.markdown(f"""
    <div class="footer">
        Dados: IF.data — Resumo, Ativo, Passivo, DRE — {period_label} ·
        DRE anualizado (4 trimestres) · Conglomerados Prudenciais
    </div>
    """, unsafe_allow_html=True)
