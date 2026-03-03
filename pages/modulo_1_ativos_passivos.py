"""
Módulo 1: Ativos e Passivos
Treemap de instituições financeiras por Ativo Total, Crédito, Captações e PL.
"""

import streamlit as st
import pandas as pd
import plotly.express as px

from config import (
    MODULO1_VARS, SEGMENT_COLORS, ALL_SEGMENTS, DEFAULT_SEGMENTS,
    RELATORIO_RESUMO, format_brl,
)
from data_utils import (
    fetch_valores, build_institution_table,
    extract_variable, apply_materiality_filter,
    find_latest_quarter, format_anomes,
)
from ui_components import render_module_header, fix_treemap_parent_hover


def render():
    """Main render function for Module 1."""

    # ─────────────────────────────────────────
    # SESSION STATE
    # ─────────────────────────────────────────
    if "mod1_selected_var" not in st.session_state:
        st.session_state.mod1_selected_var = None
    if "mod1_segments" not in st.session_state:
        st.session_state.mod1_segments = DEFAULT_SEGMENTS.copy()

    # ─────────────────────────────────────────
    # HEADER
    # ─────────────────────────────────────────
    render_module_header(
        icon="🏗️",
        title="Ativos e Passivos",
        subtitle="Ranking de instituições financeiras por variáveis patrimoniais",
    )

    # ─────────────────────────────────────────
    # VARIABLE SELECTOR (Clickable Cards)
    # ─────────────────────────────────────────
    st.markdown("##### Selecione a variável de análise:")

    var_keys = list(MODULO1_VARS.keys())
    cols = st.columns(len(var_keys))

    for i, var_key in enumerate(var_keys):
        var_info = MODULO1_VARS[var_key]

        with cols[i]:
            is_selected = st.session_state.mod1_selected_var == var_key

            if st.button(
                f"{var_info['icon']}  {var_info['label']}",
                key=f"sel_{var_key}",
                use_container_width=True,
                type="primary" if is_selected else "secondary",
            ):
                st.session_state.mod1_selected_var = var_key
                st.rerun()

    selected_var = st.session_state.mod1_selected_var
    selected_info = MODULO1_VARS.get(selected_var) if selected_var else None

    # ─────────────────────────────────────────
    # SEGMENT FILTER
    # ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("##### Filtro de Segmentação:")

    seg_cols = st.columns(len(ALL_SEGMENTS))
    new_segments = []
    for i, seg in enumerate(ALL_SEGMENTS):
        with seg_cols[i]:
            is_on = seg in st.session_state.mod1_segments
            if st.checkbox(seg, value=is_on, key=f"seg1_{seg}"):
                new_segments.append(seg)

    if not new_segments:
        new_segments = ALL_SEGMENTS.copy()
    st.session_state.mod1_segments = new_segments

    # ─────────────────────────────────────────
    # GUARD: wait for variable selection
    # ─────────────────────────────────────────
    if selected_info is None:
        st.info("👆 Selecione uma variável de análise acima para visualizar os dados.")
        return

    # ─────────────────────────────────────────
    # DATA LOADING
    # ─────────────────────────────────────────
    st.markdown("---")

    anomes = find_latest_quarter()

    with st.spinner(f"Carregando dados do IF.data ({format_anomes(anomes)})..."):
        # Build institution table (filtered by segment + PRUDENCIAL)
        institutions = build_institution_table(anomes)

        if institutions.empty:
            st.error("❌ Não foi possível carregar os dados de cadastro do IF.data.")
            return

        # Fetch report data (TipoInstituicao=1)
        relatorio = selected_info["relatorio"]
        raw_df = fetch_valores(anomes, tipo=1, relatorio=relatorio)

        # Also fetch Resumo for materiality filter (if not already)
        if relatorio != RELATORIO_RESUMO:
            resumo_df = fetch_valores(anomes, tipo=1, relatorio=RELATORIO_RESUMO)
        else:
            resumo_df = raw_df

        if raw_df.empty:
            st.error("❌ Não foi possível carregar os dados do IF.data.")
            return

        # Extract variable (long format: NomeColuna + Saldo)
        data = extract_variable(raw_df, selected_info["conta"], institutions)

        if data.empty:
            st.warning(f"⚠️ Nenhum dado encontrado para '{selected_info['label']}'.")
            return

        # Apply materiality filter
        data = apply_materiality_filter(data, resumo_df, institutions)

        # Filter by selected segments
        data = data[data["Segmento_Calculado"].isin(st.session_state.mod1_segments)].copy()

        # Final cleanup: remove any NaN/zero/invalid
        data = data.dropna(subset=["Saldo"])
        data = data[data["Saldo"].abs() > 0].copy()

        if data.empty:
            st.warning("⚠️ Nenhuma instituição atende aos critérios selecionados.")
            return

    # ─────────────────────────────────────────
    # SUMMARY METRICS
    # ─────────────────────────────────────────
    total = data["Saldo"].sum()
    n_insts = len(data)
    top5 = data.nlargest(5, "Saldo")
    top5_share = top5["Saldo"].sum() / total * 100 if total else 0

    mcol1, mcol2, mcol3, mcol4 = st.columns(4)
    with mcol1:
        st.metric("Total do Sistema", format_brl(total))
    with mcol2:
        st.metric("Nº de Instituições", f"{n_insts}")
    with mcol3:
        st.metric("Top 5 (% do total)", f"{top5_share:.1f}%")
    with mcol4:
        st.metric("Período", format_anomes(anomes))

    # ─────────────────────────────────────────
    # TREEMAP
    # ─────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"### {selected_info['icon']} {selected_info['label']}")

    plot_data = data.copy()
    plot_data["Saldo_Abs"] = plot_data["Saldo"].abs()
    plot_data["Pct"] = (plot_data["Saldo_Abs"] / plot_data["Saldo_Abs"].sum() * 100)
    plot_data["Saldo_Fmt"] = plot_data["Saldo"].apply(format_brl)

    # Compute rank (1 = largest)
    plot_data["Rank"] = plot_data["Saldo_Abs"].rank(ascending=False, method="min").astype(int)

    # All banks get their short name as label (ensure unique)
    name_counts = plot_data["NomeReduzido"].value_counts()
    dupes = set(name_counts[name_counts > 1].index)
    plot_data["Label_Treemap"] = plot_data.apply(
        lambda r: f"{r['NomeReduzido']} ({r['CodInst'][-4:]})" if r["NomeReduzido"] in dupes else r["NomeReduzido"],
        axis=1,
    )

    color_map = {
        seg: SEGMENT_COLORS.get(seg, "#718096")
        for seg in plot_data["Segmento_Calculado"].unique()
    }

    fig = px.treemap(
        plot_data,
        path=["Segmento_Calculado", "Label_Treemap"],
        values="Saldo_Abs",
        color="Segmento_Calculado",
        color_discrete_map=color_map,
        custom_data=["NomeReduzido", "Saldo_Fmt", "Pct", "Rank"],
    )

    fig.update_traces(
        textinfo="label+percent parent",
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Rank: %{customdata[3]}º<br>"
            "%{customdata[1]}<br>"
            "%{customdata[2]:.2f}% do total<br>"
            "%{percentParent:.1%} do segmento"
            "<extra></extra>"
        ),
        textfont=dict(size=13, family="DM Sans"),
        marker=dict(
            cornerradius=3,
            line=dict(width=1.5, color="white"),
        ),
    )

    fig.update_layout(
        height=650,
        margin=dict(t=30, l=10, r=10, b=10),
        font=dict(family="Space Grotesk", color="#f1f5f9"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    fix_treemap_parent_hover(fig)

    try:
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao renderizar treemap: {e}")
        st.dataframe(plot_data[["NomeReduzido", "Segmento_Calculado", "Saldo_Fmt", "Rank"]].head(20))

    # ─────────────────────────────────────────
    # TOP 20 TABLE
    # ─────────────────────────────────────────
    st.markdown("#### 🏆 Top 20 Instituições")

    top20 = data.nlargest(20, "Saldo")[
        ["NomeReduzido", "NomeDisplay", "Segmento_Calculado", "Saldo"]
    ].reset_index(drop=True)
    top20["Saldo_Fmt"] = top20["Saldo"].apply(format_brl)
    top20["Pct"] = (top20["Saldo"] / total * 100).round(2)

    # Build styled HTML table
    seg_colors = {
        "S1": "#22d3ee", "S2": "#34d399", "S3": "#fbbf24", "S4": "#a78bfa",
        "N1": "#fb7185", "N2": "#f97316", "N4": "#38bdf8",
    }

    header_html = f"""
    <tr>
        <th style="width:45px; text-align:center;">#</th>
        <th>INSTITUIÇÃO</th>
        <th style="text-align:center; width:100px;">SEGMENTO</th>
        <th style="text-align:right;">{selected_info['label'].upper()}</th>
        <th style="text-align:right; width:90px;">% TOTAL</th>
        <th style="width:180px;"></th>
    </tr>"""

    rows_html = ""
    max_saldo = top20["Saldo"].max() if not top20.empty else 1
    for i, (_, row) in enumerate(top20.iterrows()):
        rank = i + 1
        seg = row["Segmento_Calculado"]
        seg_color = seg_colors.get(seg, "#718096")
        bar_width = row["Saldo"] / max_saldo * 100

        # Medal for top 3
        medal = ""
        if rank == 1:
            medal = "🥇"
        elif rank == 2:
            medal = "🥈"
        elif rank == 3:
            medal = "🥉"

        rows_html += f"""
        <tr>
            <td class="rank-cell">
                {medal if medal else rank}
            </td>
            <td>
                <div class="bank-name">{row['NomeReduzido']}</div>
                <div class="bank-full">{row['NomeDisplay']}</div>
            </td>
            <td style="text-align:center;">
                <span class="seg-pill" style="
                    background:{seg_color}18;
                    color:{seg_color};
                ">{seg}</span>
            </td>
            <td class="value-cell">
                {row['Saldo_Fmt']}
            </td>
            <td class="pct-cell">
                {row['Pct']:.2f}%
            </td>
            <td style="padding: 8px 12px;">
                <div style="
                    background: linear-gradient(90deg, {seg_color}30 {bar_width:.1f}%, transparent {bar_width:.1f}%);
                    height: 6px;
                    border-radius: 3px;
                    width: 100%;
                "></div>
            </td>
        </tr>"""

    st.markdown(
        f"""
        <table class="top20-table">
            <thead>{header_html}</thead>
            <tbody>{rows_html}</tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

    # ─────────────────────────────────────────
    # SEARCH
    # ─────────────────────────────────────────
    st.markdown("---")
    search_query = st.text_input(
        "🔍 Buscar instituição",
        placeholder="Digite o nome do banco...",
        key="mod1_search",
    )

    if search_query:
        mask = data["NomeDisplay"].str.contains(search_query, case=False, na=False)
        search_results = data[mask].copy()
        if not search_results.empty:
            search_results["Rank"] = data["Saldo"].rank(ascending=False, method="min").astype(int)
            search_results = search_results.sort_values("Saldo", ascending=False)
            st.info(f"🔍 {len(search_results)} resultado(s) para '{search_query}'")
            display_df = search_results[["NomeReduzido", "Segmento_Calculado", "Saldo", "Rank"]].copy()
            display_df["Saldo"] = display_df["Saldo"].apply(format_brl)
            st.dataframe(
                display_df.reset_index(drop=True)
                .rename(columns={
                    "NomeReduzido": "Instituição",
                    "Segmento_Calculado": "Segmento",
                    "Saldo": selected_info["label"],
                    "Rank": "Posição",
                }),
                use_container_width=True,
            )
        else:
            st.warning(f"Nenhuma instituição encontrada para '{search_query}'.")

    # Footer
    st.markdown("---")
    st.markdown(f"""
    <div class="footer">
        Dados: IF.data — Relatório {selected_info['relatorio']} — {format_anomes(anomes)} ·
        Critérios: Ativos ≥ R$ 100M, PL ≥ R$ 20M, Saldo ≠ 0, Conglomerados Prudenciais
    </div>
    """, unsafe_allow_html=True)
