"""
Hub Central: página inicial com cards dos módulos.
"""

import streamlit as st
from config import MODULES, APP_TITLE, APP_SUBTITLE, APP_ICON
from ui_components import inject_global_css

inject_global_css()

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.markdown(f"""
<div class="app-header">
    <h1>{APP_ICON} {APP_TITLE}</h1>
    <p>{APP_SUBTITLE}</p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")
st.markdown("### Módulos de Análise")
st.markdown("Selecione um módulo no menu lateral para iniciar a análise.")

# ─────────────────────────────────────────────
# MODULE CARDS (informational, navigation via sidebar)
# ─────────────────────────────────────────────
module_keys = list(MODULES.keys())

for row_start in range(0, len(module_keys), 4):
    cols = st.columns(4)
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
            <br>
            """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align: center; color: #a0aec0; font-size: 0.78rem; padding: 12px 0;">
    Dados: <a href="https://dadosabertos.bcb.gov.br/" target="_blank" style="color: #3b82f6;">
    Portal de Dados Abertos do Banco Central do Brasil</a> — IF.data<br>
    Desenvolvido para fins educacionais — COPPEAD/UFRJ
</div>
""", unsafe_allow_html=True)
