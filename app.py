"""
Laboratório de Dados Públicos - BCB
Hub Central + Router — Dark Cyan Financial Terminal Theme
"""

import streamlit as st

st.set_page_config(
    page_title="Laboratório de Dados Públicos",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from config import MODULES, APP_TITLE, APP_SUBTITLE, APP_ICON
from ui_components import inject_global_css

inject_global_css()

# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
if "current_page" not in st.session_state:
    st.session_state.current_page = "hub"


def go_to(page: str):
    st.session_state.current_page = page


# ─────────────────────────────────────────────
# TOP BAR
# ─────────────────────────────────────────────
col_logo, col_back = st.columns([9, 1])
with col_logo:
    st.markdown(f"""
    <div style="display:flex; align-items:center; gap:14px;">
        <div style="
            font-family: 'Space Mono', monospace;
            font-size: 14px;
            font-weight: 700;
            color: #000000 !important;
            background: linear-gradient(135deg, #22d3ee, #06b6d4);
            padding: 8px 14px;
            border-radius: 10px;
            letter-spacing: 0.05em;
        ">LDP</div>
        <div>
            <div style="font-size:1rem; font-weight:700; color:#f1f5f9; font-family:'Space Grotesk',sans-serif;">
                {APP_TITLE}
            </div>
            <div style="font-size:0.75rem; color:#64748b; font-family:'Space Grotesk',sans-serif;">
                {APP_SUBTITLE}
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
with col_back:
    if st.session_state.current_page != "hub":
        if st.button("← Home", use_container_width=True):
            go_to("hub")
            st.rerun()

st.markdown("<hr style='border-color: rgba(34,211,238,0.08); margin: 12px 0 24px;'>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# ROUTER
# ─────────────────────────────────────────────
if st.session_state.current_page == "1_ativos_passivos":
    from pages import modulo_1_ativos_passivos
    modulo_1_ativos_passivos.render()

elif st.session_state.current_page == "2_resultado":
    from pages import modulo_2_resultado
    modulo_2_resultado.render()

elif st.session_state.current_page == "3_credito_pf":
    from pages import modulo_3_credito_pf
    modulo_3_credito_pf.render()

elif st.session_state.current_page == "4_credito_pj":
    from pages import modulo_4_credito_pj
    modulo_4_credito_pj.render()

elif st.session_state.current_page == "5_taxas_juros":
    from pages import modulo_5_taxas_juros
    modulo_5_taxas_juros.render()

elif st.session_state.current_page == "6_credito_regiao":
    from pages import modulo_6_credito_regiao
    modulo_6_credito_regiao.render()

elif st.session_state.current_page == "7_indices":
    from pages import modulo_7_indices
    modulo_7_indices.render()

elif st.session_state.current_page == "8_cartograma":
    from pages import modulo_8_cartograma
    modulo_8_cartograma.render()

elif st.session_state.current_page == "feedback":
    from pages import modulo_feedback
    modulo_feedback.render()

else:
    # =========================================================================
    # HUB
    # =========================================================================

    # Hero section
    st.markdown("""
    <div style="display:flex; flex-direction:column; align-items:center; padding: 20px 0 12px;">
        <div class="api-badge">API DADOS ABERTOS BCB</div>
        <h1 class="hub-title">Laboratório de Dados Públicos</h1>
        <p class="hub-desc">
            Explore, visualize e exporte dados do Portal de Dados Abertos do Banco Central do Brasil.
            Consulte APIs oficiais e gere relatórios personalizados em poucos cliques.
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Module cards
    module_keys = list(MODULES.keys())

    for row_start in range(0, len(module_keys), 4):
        cols = st.columns(4, gap="small")
        for i, col in enumerate(cols):
            idx = row_start + i
            if idx >= len(module_keys):
                break
            key = module_keys[idx]
            mod = MODULES[key]

            with col:
                viz = mod.get("viz_type", "custom")
                badge_class = f"badge-{viz}"

                st.markdown(f"""
                <div class="hub-card">
                    <div class="hub-card-icon">{mod['icon']}</div>
                    <div class="hub-card-title">{mod['title']}</div>
                    <div class="hub-card-desc">{mod['description']}</div>
                    <span class="hub-card-badge {badge_class}">{viz.upper()}</span>
                </div>
                """, unsafe_allow_html=True)

                if st.button(
                    f"{mod['icon']} {mod['title']}",
                    key=f"hub_{key}",
                    use_container_width=True,
                    type="primary",
                ):
                    go_to(key)
                    st.rerun()

    # Feedback button
    st.markdown("<br>", unsafe_allow_html=True)
    _, col_fb, _ = st.columns([3, 2, 3])
    with col_fb:
        if st.button("💬 Sugestões e Feedback", key="hub_feedback", use_container_width=True):
            go_to("feedback")
            st.rerun()

    # Footer
    st.markdown("""
    <div class="footer">
        Dados:
        <a href="https://dadosabertos.bcb.gov.br/" target="_blank">dadosabertos.bcb.gov.br</a>
        · IF.Data API · python-bcb
        <br>Desenvolvido para fins educacionais — COPPEAD/UFRJ
    </div>
    """, unsafe_allow_html=True)
