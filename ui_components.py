"""
Laboratório de Dados Públicos - BCB
UI Components — Dark Cyan Financial Terminal Theme
Matching the existing Laboratório de Dados Públicos visual identity.
"""

import streamlit as st


def inject_global_css():
    """Inject the dark cyan financial terminal theme."""
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');

    /* ── CSS Variables ── */
    :root {
        --bg-primary: #0a0f1a;
        --bg-card: #111827;
        --bg-card-hover: #1a2332;
        --bg-surface: #0d1321;
        --border-color: rgba(34,211,238,0.12);
        --border-hover: rgba(34,211,238,0.3);
        --text-primary: #f1f5f9;
        --text-secondary: #94a3b8;
        --text-muted: #64748b;
        --accent-cyan: #22d3ee;
        --accent-cyan-dim: rgba(34,211,238,0.15);
        --accent-emerald: #34d399;
        --accent-rose: #fb7185;
        --accent-amber: #fbbf24;
        --accent-violet: #a78bfa;
        --font-display: 'Space Grotesk', sans-serif;
        --font-mono: 'Space Mono', monospace;
    }

    /* ── Global Dark Theme ── */
    .stApp {
        background: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        font-family: var(--font-display) !important;
    }

    /* Force dark background on all containers */
    .stApp > header { background: transparent !important; }
    section[data-testid="stSidebar"] {
        background: var(--bg-surface) !important;
        border-right: 1px solid var(--border-color) !important;
    }
    .block-container { max-width: 1200px; }

    /* ── Typography ── */
    h1, h2, h3, h4, h5, h6 {
        font-family: var(--font-display) !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.02em;
    }
    p, span, div, label {
        color: var(--text-secondary) !important;
    }

    /* ── API Badge ── */
    .api-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 16px;
        border: 1px solid var(--border-color);
        border-radius: 99px;
        font-family: var(--font-mono);
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
        color: var(--accent-cyan);
        background: var(--accent-cyan-dim);
        margin-bottom: 16px;
    }
    .api-badge::before {
        content: '';
        width: 6px; height: 6px;
        border-radius: 50%;
        background: var(--accent-cyan);
        box-shadow: 0 0 8px var(--accent-cyan);
    }

    /* ── Hub Title ── */
    .hub-title {
        font-family: var(--font-display) !important;
        font-size: 2.6rem !important;
        font-weight: 700 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.03em;
        margin: 8px 0 12px !important;
        line-height: 1.1;
    }
    .hub-desc {
        font-size: 0.95rem;
        color: var(--text-secondary) !important;
        max-width: 600px;
        margin: 0 auto;
        line-height: 1.5;
    }

    /* ── Module Cards (Hub) ── */
    .hub-card {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 16px;
        padding: 22px 20px;
        min-height: 170px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    .hub-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, transparent, var(--accent-cyan), transparent);
        opacity: 0;
        transition: opacity 0.3s;
    }
    .hub-card:hover {
        border-color: var(--border-hover);
        background: var(--bg-card-hover);
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(34,211,238,0.06);
    }
    .hub-card:hover::before { opacity: 1; }
    .hub-card-icon {
        font-size: 2rem;
        margin-bottom: 16px;
        display: inline-block;
        padding: 8px;
        background: var(--accent-cyan-dim);
        border-radius: 12px;
    }
    .hub-card-title {
        font-family: var(--font-display);
        font-size: 1.05rem;
        font-weight: 700;
        color: var(--text-primary) !important;
        margin-bottom: 8px;
    }
    .hub-card-desc {
        font-size: 0.82rem;
        color: var(--text-muted) !important;
        line-height: 1.5;
    }
    .hub-card-badge {
        display: inline-block;
        margin-top: 14px;
        padding: 3px 12px;
        border-radius: 99px;
        font-family: var(--font-mono);
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    .badge-treemap { background: rgba(34,211,238,0.12); color: var(--accent-cyan); }
    .badge-barras { background: rgba(251,191,36,0.12); color: var(--accent-amber); }
    .badge-cartograma { background: rgba(52,211,153,0.12); color: var(--accent-emerald); }
    .badge-custom { background: rgba(167,139,250,0.12); color: var(--accent-violet); }

    /* ── Streamlit Button Override ── */
    /* Base style for ALL buttons */
    .stButton > button,
    button[data-testid="stBaseButton-primary"],
    button[data-testid="stBaseButton-secondary"],
    button[data-testid="stBaseButton-primaryFormSubmit"] {
        font-family: var(--font-display) !important;
        font-weight: 600 !important;
        border-radius: 10px !important;
        transition: all 0.2s ease !important;
        font-size: 14px !important;
    }

    /* PRIMARY buttons: cyan bg, dark text */
    button[data-testid="stBaseButton-primary"],
    button[data-testid="stBaseButton-primaryFormSubmit"],
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #22d3ee, #06b6d4) !important;
        color: #0a0f1a !important;
        border: none !important;
        font-weight: 700 !important;
    }
    button[data-testid="stBaseButton-primary"] p,
    button[data-testid="stBaseButton-primary"] span,
    button[data-testid="stBaseButton-primary"] div,
    button[data-testid="stBaseButton-primaryFormSubmit"] p,
    button[data-testid="stBaseButton-primaryFormSubmit"] span {
        color: #0a0f1a !important;
    }
    button[data-testid="stBaseButton-primary"]:hover {
        box-shadow: 0 4px 20px rgba(34,211,238,0.3) !important;
        transform: translateY(-1px);
    }

    /* SECONDARY buttons: dark bg, LIGHT text */
    button[data-testid="stBaseButton-secondary"],
    .stButton > button[kind="secondary"] {
        background: #111827 !important;
        color: #e2e8f0 !important;
        border: 1px solid rgba(34,211,238,0.25) !important;
    }
    button[data-testid="stBaseButton-secondary"]:hover {
        border-color: rgba(34,211,238,0.5) !important;
        color: #22d3ee !important;
        background: #1a2332 !important;
    }
    /* Force light text inside secondary button spans/ps */
    button[data-testid="stBaseButton-secondary"] p,
    button[data-testid="stBaseButton-secondary"] span,
    button[data-testid="stBaseButton-secondary"] div {
        color: #e2e8f0 !important;
    }
    button[data-testid="stBaseButton-secondary"]:hover p,
    button[data-testid="stBaseButton-secondary"]:hover span {
        color: #22d3ee !important;
    }

    /* Disabled buttons */
    .stButton > button:disabled,
    button[data-testid="stBaseButton-secondary"]:disabled {
        background: #0d1321 !important;
        color: #475569 !important;
        border: 1px solid rgba(148,163,184,0.08) !important;
        opacity: 0.6 !important;
    }
    button:disabled p, button:disabled span {
        color: #475569 !important;
    }

    /* ── Variable Selector Cards ── */
    .var-card {
        background: var(--bg-card);
        border: 2px solid var(--border-color);
        border-radius: 14px;
        padding: 18px 12px;
        text-align: center;
        transition: all 0.25s ease;
        margin-bottom: 6px;
    }
    .var-card.selected {
        border-color: var(--accent-cyan);
        background: rgba(34,211,238,0.08);
        box-shadow: 0 0 0 3px rgba(34,211,238,0.1);
    }
    .var-card-icon { font-size: 2rem; margin-bottom: 6px; }
    .var-card-label {
        font-size: 0.9rem;
        font-weight: 700;
        color: var(--text-primary) !important;
    }
    .var-card-desc {
        font-size: 0.72rem;
        color: var(--text-muted) !important;
        margin-top: 4px;
    }

    /* ── Segment Checkboxes ── */
    .stCheckbox label span {
        color: var(--text-secondary) !important;
        font-family: var(--font-mono) !important;
        font-size: 13px !important;
    }

    /* ── Metrics ── */
    [data-testid="stMetric"] {
        background: var(--bg-card);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        padding: 16px !important;
    }
    [data-testid="stMetricValue"] {
        color: var(--accent-cyan) !important;
        font-family: var(--font-mono) !important;
        font-size: 1.3rem !important;
    }
    [data-testid="stMetricLabel"] {
        color: var(--text-muted) !important;
        font-family: var(--font-display) !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }

    /* ── Text Input ── */
    .stTextInput input {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 10px !important;
        color: var(--text-primary) !important;
        font-family: var(--font-display) !important;
    }
    .stTextInput input:focus {
        border-color: var(--accent-cyan) !important;
        box-shadow: 0 0 0 2px rgba(34,211,238,0.15) !important;
    }

    /* ── Top 20 Table ── */
    .top20-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        font-family: var(--font-display);
        border: 1px solid var(--border-color);
        border-radius: 12px;
        overflow: hidden;
        background: var(--bg-card);
    }
    .top20-table th {
        padding: 14px 16px;
        font-family: var(--font-mono);
        font-size: 10px;
        font-weight: 700;
        color: var(--accent-cyan);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        background: var(--bg-surface);
        border-bottom: 1px solid var(--border-color);
        text-align: left;
    }
    .top20-table td {
        padding: 12px 16px;
        border-bottom: 1px solid rgba(34,211,238,0.06);
        vertical-align: middle;
        font-size: 13px;
    }
    .top20-table tbody tr:hover {
        background: rgba(34,211,238,0.06);
    }
    .top20-table tbody tr:last-child td {
        border-bottom: none;
    }
    .top20-table .bank-name {
        font-weight: 700;
        color: #e2e8f0;
        font-size: 13px;
    }
    .top20-table .bank-full {
        font-size: 11px;
        color: #64748b;
        margin-top: 2px;
    }
    .top20-table .value-cell {
        text-align: right;
        font-family: var(--font-mono);
        font-weight: 700;
        color: #f1f5f9;
        font-size: 13px;
    }
    .top20-table .pct-cell {
        text-align: right;
        font-family: var(--font-mono);
        font-weight: 600;
        color: var(--accent-cyan);
        font-size: 12px;
    }
    .top20-table .rank-cell {
        text-align: center;
        font-weight: 700;
        color: #94a3b8;
        font-family: var(--font-mono);
        font-size: 14px;
    }
    .seg-pill {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 99px;
        font-size: 11px;
        font-weight: 700;
        font-family: var(--font-mono);
        letter-spacing: 0.03em;
    }

    /* ── Section Title ── */
    .section-title {
        font-family: var(--font-display);
        font-size: 20px;
        font-weight: 700;
        color: var(--text-primary) !important;
        margin: 24px 0 12px;
    }

    /* ── Module Header ── */
    .module-header {
        display: flex;
        align-items: center;
        gap: 14px;
        margin-bottom: 24px;
    }
    .module-header-icon {
        font-size: 2rem;
        padding: 10px;
        background: var(--accent-cyan-dim);
        border-radius: 14px;
    }
    .module-header-title {
        font-family: var(--font-display);
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--text-primary) !important;
        letter-spacing: -0.02em;
    }
    .module-header-sub {
        font-size: 0.85rem;
        color: var(--text-muted) !important;
    }

    /* ── Footer ── */
    .footer {
        text-align: center;
        padding: 20px 0;
        font-size: 12px;
        color: var(--text-muted) !important;
        font-family: var(--font-mono);
    }
    .footer a {
        color: var(--accent-cyan) !important;
        text-decoration: none;
    }

    /* ── Dividers ── */
    hr {
        border-color: var(--border-color) !important;
    }

    /* ── Hide Streamlit defaults ── */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
    header { visibility: hidden; }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: var(--bg-primary); }
    ::-webkit-scrollbar-thumb { background: var(--border-color); border-radius: 3px; }
    </style>
    """, unsafe_allow_html=True)


def render_module_header(icon: str, title: str, subtitle: str = ""):
    """Render module page header."""
    st.markdown(f"""
    <div class="module-header">
        <div class="module-header-icon">{icon}</div>
        <div>
            <div class="module-header-title">{title}</div>
            <div class="module-header-sub">{subtitle}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def show_loading(message: str = "Carregando dados do Banco Central..."):
    """Show loading indicator."""
    return st.spinner(message)


def fix_treemap_parent_hover(fig):
    """
    Fix treemap hover for parent (segment) nodes.
    Parent nodes get auto-generated customdata with None/NaN values.
    Replace their hover to show only segment name + % do total.
    """
    import numpy as np
    for trace in fig.data:
        if not hasattr(trace, 'customdata') or trace.customdata is None:
            continue
        cd = trace.customdata
        labels = trace.labels if hasattr(trace, 'labels') else [None] * len(cd)
        parents = trace.parents if hasattr(trace, 'parents') else [None] * len(cd)
        values = trace.values if hasattr(trace, 'values') else [0] * len(cd)
        total_val = sum(v for v in values if v and not np.isnan(v))

        hover_texts = []
        for i, row in enumerate(cd):
            name = str(row[0]) if (row[0] is not None and str(row[0]) != 'nan') else None

            if name and name not in ('', '?', '(?)'):
                # Leaf node (institution)
                rank = row[3] if len(row) > 3 and row[3] is not None else ''
                saldo = row[1] if len(row) > 1 and row[1] is not None else ''
                try:
                    pct = float(row[2]) if len(row) > 2 and row[2] is not None else 0
                    pct_str = f"{pct:.2f}% do total"
                except (ValueError, TypeError):
                    pct_str = ""

                # Compute % of segment (percentParent)
                parent_name = parents[i] if i < len(parents) else None
                if parent_name and parent_name != '(?)':
                    # Sum values in same parent
                    parent_total = sum(
                        values[j] for j in range(len(parents))
                        if parents[j] == parent_name and values[j] and not np.isnan(values[j])
                    )
                    seg_pct = (values[i] / parent_total * 100) if parent_total > 0 else 0
                    seg_str = f"{seg_pct:.1f}% do segmento"
                else:
                    seg_str = ""

                hover_texts.append(
                    f"<b>{name}</b><br>"
                    f"Rank: {rank}º<br>"
                    f"{saldo}<br>"
                    f"{pct_str}<br>"
                    f"{seg_str}"
                )
            else:
                # Parent node (segment) — show segment name + % do total
                label = labels[i] if i < len(labels) else ''
                val = values[i] if i < len(values) and values[i] else 0
                try:
                    total_pct = (val / total_val * 100) if total_val > 0 else 0
                except (TypeError, ZeroDivisionError):
                    total_pct = 0
                hover_texts.append(
                    f"<b>{label}</b><br>"
                    f"{total_pct:.1f}% do total"
                )

        trace.hovertext = hover_texts
        trace.hovertemplate = "%{hovertext}<extra></extra>"
    return fig
