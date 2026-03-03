"""
Módulo 8: Cartograma — Crédito Total por Região
Mapa do Brasil com regiões que se deformam proporcionalmente ao crédito.
Dados: Relatório 9 (Crédito por Região Geográfica).
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json

from config import (
    RELATORIO_RESUMO, RELATORIO_CREDITO_GEO,
    format_brl,
)
from data_utils import (
    fetch_valores, build_institution_table,
    find_latest_quarter, format_anomes,
)
from ui_components import render_module_header


# ─────────────────────────────────────────────
# REGION CONFIG
# ─────────────────────────────────────────────
REGIOES = {
    "Norte":        {"color": "#10b981", "nome_api": "Norte"},
    "Nordeste":     {"color": "#f59e0b", "nome_api": "Nordeste"},
    "Centro-Oeste": {"color": "#ef4444", "nome_api": "Centro-oeste"},
    "Sudeste":      {"color": "#22d3ee", "nome_api": "Sudeste"},
    "Sul":          {"color": "#a78bfa", "nome_api": "Sul"},
}


def _extract_region_totals(raw_df: pd.DataFrame) -> dict:
    """Sum Saldo per region across all institutions."""
    results = {}
    for reg_name, reg_info in REGIOES.items():
        api_name = reg_info["nome_api"]
        mask = raw_df["NomeColuna"] == api_name
        sub = raw_df[mask].copy()
        sub["Saldo"] = pd.to_numeric(sub["Saldo"], errors="coerce")
        total = sub["Saldo"].sum()
        results[reg_name] = total
    return results


def _build_cartogram_html(region_data: dict, period: str) -> str:
    """Build Dorling cartogram — proportional circles on Brazil outline."""

    total = sum(region_data.values())
    if total == 0:
        total = 1

    # Centroids (approximate) for each region in SVG coords (viewBox 0 0 600 600)
    centroids = {
        "Norte":        (255, 135),
        "Nordeste":     (430, 195),
        "Centro-Oeste": (290, 315),
        "Sudeste":      (395, 395),
        "Sul":          (320, 490),
    }

    # Max radius and min radius
    max_r = 110
    min_r = 30

    # Compute radii proportional to sqrt(value) so area ~ value
    import math
    values = {k: v for k, v in region_data.items()}
    max_val = max(values.values()) if values else 1
    radii = {}
    for reg, val in values.items():
        ratio = val / max_val if max_val > 0 else 0
        radii[reg] = min_r + (max_r - min_r) * math.sqrt(ratio)

    # Build circle elements
    circles_svg = ""
    labels_svg = ""
    for reg_name, (cx, cy) in centroids.items():
        r = radii.get(reg_name, min_r)
        color = REGIOES[reg_name]["color"]
        val = values.get(reg_name, 0)
        share = val / total * 100
        formatted = format_brl(val)

        circles_svg += f'''
        <circle class="dorling-circle" id="circ-{reg_name}" data-region="{reg_name}"
                cx="{cx}" cy="{cy}" r="0"
                data-target-r="{r:.1f}"
                fill="{color}" fill-opacity="0.8"
                stroke="{color}" stroke-width="2" stroke-opacity="0.4"/>
        '''

        labels_svg += f'''
        <text class="circ-label" x="{cx}" y="{cy - 8}" text-anchor="middle"
              font-size="{max(11, min(15, r/6))}" opacity="0"
              data-region="{reg_name}">{reg_name.upper()}</text>
        <text class="circ-value" x="{cx}" y="{cy + 10}" text-anchor="middle"
              font-size="{max(9, min(13, r/7))}" opacity="0"
              data-region="{reg_name}">{share:.1f}%</text>
        <text class="circ-amount" x="{cx}" y="{cy + 25}" text-anchor="middle"
              font-size="{max(8, min(11, r/8))}" opacity="0"
              data-region="{reg_name}">{formatted}</text>
        '''

    data_json = json.dumps({
        reg: {
            "value": values[reg],
            "share": round(values[reg] / total * 100, 1),
            "formatted": format_brl(values[reg]),
            "color": REGIOES[reg]["color"],
            "radius": round(radii[reg], 1),
        }
        for reg in REGIOES
    }, ensure_ascii=False)

    total_fmt = format_brl(total)

    html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=Space+Mono:wght@400;700&display=swap');

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    background: transparent;
    font-family: 'Space Grotesk', sans-serif;
    color: #f1f5f9;
    overflow: hidden;
  }}

  .container {{
    width: 100%;
    padding: 10px 20px;
    position: relative;
  }}

  .total-display {{
    text-align: center;
    margin-bottom: 10px;
  }}

  .total-label {{
    font-size: 11px;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-family: 'Space Mono', monospace;
  }}

  .total-value {{
    font-size: 26px;
    font-weight: 700;
    color: #22d3ee;
    font-family: 'Space Mono', monospace;
  }}

  .map-wrap {{
    position: relative;
    width: 100%;
    display: flex;
    justify-content: center;
  }}

  .map-svg {{
    width: 100%;
    max-width: 600px;
    height: auto;
  }}

  /* Brazil outline — very faint */
  .brazil-outline {{
    fill: none;
    stroke: #1e293b;
    stroke-width: 1.5;
    opacity: 0.5;
  }}

  .brazil-fill {{
    fill: #111827;
    stroke: none;
    opacity: 0.4;
  }}

  .dorling-circle {{
    cursor: pointer;
    transition: r 1.2s cubic-bezier(0.34, 1.56, 0.64, 1),
                fill-opacity 0.3s ease,
                filter 0.3s ease;
  }}

  .dorling-circle:hover {{
    fill-opacity: 1;
    filter: drop-shadow(0 0 20px rgba(255,255,255,0.15));
  }}

  .circ-label {{
    font-family: 'Space Mono', monospace;
    font-weight: 700;
    fill: #f1f5f9;
    pointer-events: none;
    transition: opacity 0.5s ease 0.8s;
  }}

  .circ-value {{
    font-family: 'Space Mono', monospace;
    font-weight: 700;
    fill: #e2e8f0;
    pointer-events: none;
    transition: opacity 0.5s ease 1s;
  }}

  .circ-amount {{
    font-family: 'Space Mono', monospace;
    font-weight: 400;
    fill: #94a3b8;
    pointer-events: none;
    transition: opacity 0.5s ease 1.1s;
  }}

  .tooltip {{
    position: absolute;
    background: #111827;
    border: 1px solid rgba(34,211,238,0.3);
    border-radius: 8px;
    padding: 12px 16px;
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.2s ease;
    z-index: 100;
    min-width: 180px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5);
  }}

  .tooltip.active {{ opacity: 1; }}
  .tooltip-name {{ font-size: 14px; font-weight: 700; margin-bottom: 4px; }}
  .tooltip-value {{ font-size: 20px; font-weight: 700; font-family: 'Space Mono'; color: #22d3ee; }}
  .tooltip-share {{ font-size: 12px; color: #94a3b8; margin-top: 2px; }}
  .tooltip-bar {{
    margin-top: 8px;
    height: 4px;
    border-radius: 2px;
    background: #1e293b;
    overflow: hidden;
  }}
  .tooltip-bar-fill {{
    height: 100%;
    border-radius: 2px;
    transition: width 0.4s ease;
  }}

  .period-label {{
    text-align: center;
    font-size: 10px;
    color: #475569;
    font-family: 'Space Mono', monospace;
    margin-top: 6px;
  }}
</style>
</head>
<body>

<div class="container">
  <div class="total-display">
    <div class="total-label">Crédito Total por Região</div>
    <div class="total-value">{total_fmt}</div>
  </div>

  <div class="map-wrap">
    <svg class="map-svg" viewBox="0 0 600 580" preserveAspectRatio="xMidYMid meet">
      <!-- Simplified Brazil outline as background -->
      <path class="brazil-fill" d="
        M 200 60 Q 230 50, 280 55 Q 340 50, 380 60 Q 420 55, 460 70
        Q 500 85, 510 110 Q 520 140, 515 170 Q 510 200, 500 230
        Q 490 260, 480 280 Q 470 300, 460 320 Q 450 345, 440 365
        Q 430 385, 415 400 Q 400 415, 385 425 Q 370 440, 360 455
        Q 350 470, 340 485 Q 330 500, 315 510 Q 300 520, 285 515
        Q 270 505, 260 490 Q 250 475, 245 455 Q 240 440, 240 420
        Q 235 400, 230 380 Q 220 355, 215 335 Q 210 310, 200 290
        Q 190 275, 180 265 Q 170 255, 165 240 Q 155 220, 150 200
        Q 145 175, 150 150 Q 155 125, 165 105 Q 175 85, 190 70 Z"/>
      <path class="brazil-outline" d="
        M 200 60 Q 230 50, 280 55 Q 340 50, 380 60 Q 420 55, 460 70
        Q 500 85, 510 110 Q 520 140, 515 170 Q 510 200, 500 230
        Q 490 260, 480 280 Q 470 300, 460 320 Q 450 345, 440 365
        Q 430 385, 415 400 Q 400 415, 385 425 Q 370 440, 360 455
        Q 350 470, 340 485 Q 330 500, 315 510 Q 300 520, 285 515
        Q 270 505, 260 490 Q 250 475, 245 455 Q 240 440, 240 420
        Q 235 400, 230 380 Q 220 355, 215 335 Q 210 310, 200 290
        Q 190 275, 180 265 Q 170 255, 165 240 Q 155 220, 150 200
        Q 145 175, 150 150 Q 155 125, 165 105 Q 175 85, 190 70 Z"/>

      <!-- Dorling circles -->
      {circles_svg}
      {labels_svg}
    </svg>

    <div class="tooltip" id="tooltip">
      <div class="tooltip-name" id="tt-name"></div>
      <div class="tooltip-value" id="tt-value"></div>
      <div class="tooltip-share" id="tt-share"></div>
      <div class="tooltip-bar"><div class="tooltip-bar-fill" id="tt-bar"></div></div>
    </div>
  </div>

  <div class="period-label">{period} · Relatório 9 · IF.data BCB</div>
</div>

<script>
const DATA = {data_json};

// Animate circles growing
function animateCircles() {{
  document.querySelectorAll('.dorling-circle').forEach((circ, i) => {{
    const targetR = parseFloat(circ.getAttribute('data-target-r'));
    setTimeout(() => {{
      circ.setAttribute('r', targetR);
    }}, 100 + i * 150);
  }});

  // Show labels after circles grow
  setTimeout(() => {{
    document.querySelectorAll('.circ-label, .circ-value, .circ-amount').forEach(el => {{
      el.setAttribute('opacity', '1');
    }});
  }}, 600);
}}

// Tooltip
const tooltip = document.getElementById('tooltip');
const ttName = document.getElementById('tt-name');
const ttValue = document.getElementById('tt-value');
const ttShare = document.getElementById('tt-share');
const ttBar = document.getElementById('tt-bar');

document.querySelectorAll('.dorling-circle').forEach(circ => {{
  circ.addEventListener('mouseenter', (e) => {{
    const name = circ.dataset.region;
    const info = DATA[name];
    if (!info) return;

    ttName.textContent = name;
    ttName.style.color = info.color;
    ttValue.textContent = info.formatted;
    ttShare.textContent = info.share + '% do crédito total';
    ttBar.style.width = info.share + '%';
    ttBar.style.background = info.color;

    tooltip.classList.add('active');

    // Highlight: grow this, shrink others
    document.querySelectorAll('.dorling-circle').forEach(c => {{
      if (c !== circ) {{
        c.style.fillOpacity = '0.25';
        c.style.strokeOpacity = '0.15';
      }}
    }});

    // Dim labels of other regions
    document.querySelectorAll('.circ-label, .circ-value, .circ-amount').forEach(el => {{
      if (el.dataset.region !== name) el.style.opacity = '0.2';
    }});
  }});

  circ.addEventListener('mousemove', (e) => {{
    const rect = document.querySelector('.map-wrap').getBoundingClientRect();
    let left = e.clientX - rect.left + 16;
    let top = e.clientY - rect.top - 10;
    // Keep tooltip in bounds
    if (left + 200 > rect.width) left = e.clientX - rect.left - 200;
    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
  }});

  circ.addEventListener('mouseleave', () => {{
    tooltip.classList.remove('active');
    document.querySelectorAll('.dorling-circle').forEach(c => {{
      c.style.fillOpacity = '0.8';
      c.style.strokeOpacity = '0.4';
    }});
    document.querySelectorAll('.circ-label, .circ-value, .circ-amount').forEach(el => {{
      el.style.opacity = '1';
    }});
  }});
}});

// Start animation
setTimeout(animateCircles, 200);
</script>

</body>
</html>
"""
    return html


# ─────────────────────────────────────────────
# MAIN RENDER
# ─────────────────────────────────────────────

def render():
    """Main render function for Module 8."""

    render_module_header(
        icon="🗺️",
        title="Cartograma — Crédito por Região",
        subtitle="Mapa do Brasil com regiões proporcionais ao volume de crédito",
    )

    anomes = find_latest_quarter(tipos=[1], relatorio=RELATORIO_CREDITO_GEO)

    with st.spinner(f"Carregando dados regionais ({format_anomes(anomes)})..."):
        raw_df = fetch_valores(anomes, tipo=1, relatorio=RELATORIO_CREDITO_GEO)

        if raw_df.empty:
            st.error("❌ Não foi possível carregar os dados de crédito por região.")
            return

        region_data = _extract_region_totals(raw_df)

    total = sum(region_data.values())
    if total == 0:
        st.warning("⚠️ Nenhum dado de crédito encontrado.")
        return

    # Summary metrics
    sorted_regions = sorted(region_data.items(), key=lambda x: x[1], reverse=True)
    top_region = sorted_regions[0]

    mcol1, mcol2, mcol3 = st.columns(3)
    with mcol1:
        st.metric("Crédito Total", format_brl(total))
    with mcol2:
        st.metric("Maior Região", f"{top_region[0]} ({top_region[1]/total*100:.1f}%)")
    with mcol3:
        st.metric("Período", format_anomes(anomes))

    st.markdown("---")

    # Render cartogram
    period = format_anomes(anomes)
    html = _build_cartogram_html(region_data, period)
    components.html(html, height=680, scrolling=False)

    # Data table below
    st.markdown("#### 📊 Dados por Região")

    rows_html = ""
    seg_colors = {
        "Norte": "#10b981", "Nordeste": "#f59e0b",
        "Centro-Oeste": "#ef4444", "Sudeste": "#22d3ee", "Sul": "#a78bfa",
    }

    header_html = """
    <tr>
        <th style="width:45px; text-align:center;">#</th>
        <th>REGIÃO</th>
        <th style="text-align:right;">CRÉDITO TOTAL</th>
        <th style="text-align:right; width:90px;">% TOTAL</th>
        <th style="width:200px;"></th>
    </tr>"""

    max_val = max(region_data.values()) if region_data else 1
    for i, (reg, val) in enumerate(sorted_regions):
        rank = i + 1
        color = seg_colors.get(reg, "#718096")
        pct = val / total * 100
        bar_w = val / max_val * 100

        medal = ""
        if rank == 1: medal = "🥇"
        elif rank == 2: medal = "🥈"
        elif rank == 3: medal = "🥉"

        rows_html += f"""
        <tr>
            <td class="rank-cell">{medal if medal else rank}</td>
            <td>
                <div class="bank-name" style="display:flex;align-items:center;gap:8px;">
                    <span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:{color};"></span>
                    {reg}
                </div>
            </td>
            <td class="value-cell">{format_brl(val)}</td>
            <td class="pct-cell">{pct:.1f}%</td>
            <td style="padding: 8px 12px;">
                <div style="background: linear-gradient(90deg, {color}40 {bar_w:.1f}%, transparent {bar_w:.1f}%); height: 6px; border-radius: 3px; width: 100%;"></div>
            </td>
        </tr>"""

    st.markdown(
        f"""<table class="top20-table"><thead>{header_html}</thead><tbody>{rows_html}</tbody></table>""",
        unsafe_allow_html=True,
    )

    # Footer
    st.markdown("---")
    st.markdown(f"""
    <div class="footer">
        Dados: IF.data — Crédito por Região (Relatório 9) — {format_anomes(anomes)} ·
        Soma de todas as instituições por região
    </div>
    """, unsafe_allow_html=True)
