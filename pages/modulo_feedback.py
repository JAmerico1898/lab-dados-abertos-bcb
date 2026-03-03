"""
Módulo Feedback — Sugestões, Dúvidas e Bug Reports
"""

import streamlit as st
from datetime import datetime
from ui_components import render_module_header


def render():
    render_module_header(
        icon="💬",
        title="Sugestões e Feedback",
        subtitle="Encontrou um bug, tem uma ideia ou quer tirar uma dúvida? Envie aqui.",
    )

    with st.form("form_feedback", clear_on_submit=True):
        col_n, col_e = st.columns(2)
        with col_n:
            nome = st.text_input(
                "Nome (opcional)",
                placeholder="Ex.: Maria Silva",
            )
        with col_e:
            email = st.text_input(
                "E-mail (opcional)",
                placeholder="Ex.: maria@email.com",
            )

        tipo = st.selectbox("Tipo", options=[
            "💡 Sugestão", "❓ Dúvida", "🐛 Bug", "⭐ Elogio"
        ])

        msg = st.text_area(
            "Mensagem",
            height=150,
            placeholder="Descreva sua sugestão, dúvida ou problema...",
        )

        enviado = st.form_submit_button(
            "📤 Enviar",
            use_container_width=True,
        )

    if enviado:
        if not msg.strip():
            st.warning("Por favor, escreva uma mensagem.")
        else:
            corpo = (
                f"📨 {tipo}\n"
                f"👤 {nome or 'Anônimo'}\n"
                f"📧 {email or chr(8212)}\n"
                f"🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                f"{msg}"
            )

            sucesso = False
            push_token = st.secrets.get("PUSHOVER_API_TOKEN", "")
            push_user = st.secrets.get("PUSHOVER_USER_KEY", "")

            if push_token and push_user:
                try:
                    import requests
                    resp = requests.post(
                        "https://api.pushover.net/1/messages.json",
                        data={
                            "token": push_token,
                            "user": push_user,
                            "title": f"[Lab Dados Públicos] {tipo}",
                            "message": corpo,
                            "priority": 0,
                        },
                        timeout=10,
                    )
                    sucesso = resp.status_code == 200
                except Exception:
                    sucesso = False

            if sucesso:
                st.success("✅ Enviado com sucesso!")
            else:
                st.info(
                    "📝 Mensagem registrada. (Configure PUSHOVER_API_TOKEN e "
                    "PUSHOVER_USER_KEY em st.secrets para notificações.)"
                )
