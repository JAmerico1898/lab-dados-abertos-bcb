"""
Laboratório de Dados Públicos - BCB
Configurações, constantes e mapeamentos.
"""

# ─────────────────────────────────────────────
# APP INFO
# ─────────────────────────────────────────────
APP_TITLE = "Laboratório de Dados Públicos"
APP_SUBTITLE = "Portal de Dados Abertos do Banco Central do Brasil"
APP_ICON = "🏦"

# ─────────────────────────────────────────────
# MODULES (for Hub)
# ─────────────────────────────────────────────
MODULES = {
    "1_ativos_passivos": {
        "title": "Ativos e Passivos",
        "icon": "🏗️",
        "description": "Treemap de Ativo Total, Crédito, Captações e PL",
        "viz_type": "treemap",
    },
    "2_resultado": {
        "title": "Resultado",
        "icon": "📊",
        "description": "Treemap de Intermediação, Serviços, Despesas e Lucro",
        "viz_type": "treemap",
    },
    "3_credito_pf": {
        "title": "Crédito Pessoa Física",
        "icon": "👤",
        "description": "Treemap de Consignado, Veículos, Habitação e mais",
        "viz_type": "treemap",
    },
    "4_credito_pj": {
        "title": "Crédito Pessoa Jurídica",
        "icon": "🏢",
        "description": "Treemap de Capital de Giro, Investimento, Recebíveis",
        "viz_type": "treemap",
    },
    "5_taxas_juros": {
        "title": "Taxas de Juros",
        "icon": "💹",
        "description": "Rankings de taxas de operações de crédito",
        "viz_type": "custom",
    },
    "6_credito_regiao": {
        "title": "Crédito por Região",
        "icon": "🗺️",
        "description": "Treemap de carteira de crédito por região geográfica",
        "viz_type": "treemap",
    },
    "7_indices": {
        "title": "Índices Financeiros",
        "icon": "📈",
        "description": "Barras horizontais: Basileia, ROI, Eficiência e mais",
        "viz_type": "barras",
    },
    "8_cartograma": {
        "title": "Crédito Total por Região",
        "icon": "🇧🇷",
        "description": "Cartograma do Brasil com o total de crédito por região",
        "viz_type": "cartograma",
    },
}

# ─────────────────────────────────────────────
# SEGMENTATION
# ─────────────────────────────────────────────
# TCB values that override SR segmentation (actual API values are uppercase)
TCB_OVERRIDE = {"N1", "N2", "N4"}

# Valid SR segments (actual API values)
VALID_SR = {"S1", "S2", "S3", "S4"}

# All valid categories (union)
ALL_SEGMENTS = sorted(list(VALID_SR | TCB_OVERRIDE))
DEFAULT_SEGMENTS = ["S1", "S2"]

# Segment display names
SEGMENT_LABELS = {
    "N1": "Conglomerado Prudencial Tipo 1",
    "N2": "Conglomerado Prudencial Tipo 2",
    "N4": "Instituição Individual Tipo 4",
    "S1": "Segmento S1",
    "S2": "Segmento S2",
    "S3": "Segmento S3",
    "S4": "Segmento S4",
}

# Segment colors (vivid for dark background treemap)
SEGMENT_COLORS = {
    "N1": "#e11d48",
    "N2": "#f97316",
    "N4": "#38bdf8",
    "S1": "#0891b2",
    "S2": "#059669",
    "S3": "#d97706",
    "S4": "#7c3aed",
}

# ─────────────────────────────────────────────
# MATERIALITY THRESHOLDS
# ─────────────────────────────────────────────
MIN_ATIVO_TOTAL = 100_000_000  # R$ 100 milhões (raw value)
MIN_PL = 20_000_000            # R$ 20 milhões (raw value)

# ─────────────────────────────────────────────
# BANK SHORT NAMES (top banks)
# ─────────────────────────────────────────────
BANK_SHORT_NAMES = {
    "BCO DO BRASIL S.A.": "BB",
    "BCO BRADESCO S.A.": "Bradesco",
    "ITAÚ UNIBANCO S.A.": "Itaú",
    "CAIXA ECONOMICA FEDERAL": "Caixa",
    "BCO SANTANDER (BRASIL) S.A.": "Santander",
    "BCO BTG PACTUAL S.A.": "BTG Pactual",
    "BCO SAFRA S.A.": "Safra",
    "BCO VOTORANTIM S.A.": "Votorantim",
    "NU PAGAMENTOS S.A. - IP": "Nubank",
    "BCO CITIBANK S.A.": "Citi",
    "BCO J.P. MORGAN S.A.": "JP Morgan",
    "BANCO INTER S.A.": "Inter",
    "BCO ABC BRASIL S.A.": "ABC Brasil",
    "BCO DAYCOVAL S.A.": "Daycoval",
    "BCO BMG S.A.": "BMG",
    "BCO PAN S.A.": "PAN",
    "BCO ORIGINAL S.A.": "Original",
    "BCO C6 S.A.": "C6 Bank",
    "BCO BNP PARIBAS BRASIL S.A.": "BNP Paribas",
    "BCO COOPERATIVO DO BRASIL S.A. - BANCOOB": "Bancoob",
    "BCO COOPERATIVO SICREDI S.A.": "Sicredi",
    "BNDES": "BNDES",
    "BCO DO NORDESTE DO BRASIL S.A.": "BNB",
    "BCO DA AMAZONIA S.A.": "BASA",
    "ITAÚ UNIBANCO BM S.A.": "Itaú",
    "CAIXA ECONOMICA FEDERAL (CEF)": "Caixa",
    "BCO CREDIT SUISSE (BRL) S.A.": "Credit Suisse",
    "BCO MODAL S.A.": "Modal",
    "XP INVESTIMENTOS CCTVM S.A.": "XP",
    "BCO MERCANTIL DO BRASIL S.A.": "Mercantil",
}


def get_short_name(full_name: str) -> str:
    """Return short name if available, else truncate."""
    if full_name in BANK_SHORT_NAMES:
        return BANK_SHORT_NAMES[full_name]
    # Truncate long names
    name = full_name.replace("BCO ", "").replace("S.A.", "").strip()
    return name[:20] if len(name) > 20 else name


# ─────────────────────────────────────────────
# IF.DATA REPORT NUMBERS
# ─────────────────────────────────────────────
RELATORIO_RESUMO = 1
RELATORIO_ATIVO = 2
RELATORIO_PASSIVO = 3
RELATORIO_RESULTADO = 4
RELATORIO_CREDITO_PF = 11
RELATORIO_CREDITO_PJ = 13
RELATORIO_CREDITO_GEO = 9

# TipoInstituicao: we fetch all types and filter by segment
TIPO_INSTITUICAO_TODAS = [1, 2, 3]  # Bancos, Cooperativas, Financeiras

# ─────────────────────────────────────────────
# MODULE 1: ATIVOS E PASSIVOS — Variable definitions
# ─────────────────────────────────────────────
MODULO1_VARS = {
    "ativo_total": {
        "label": "Ativo Total",
        "icon": "🏦",
        "relatorio": RELATORIO_RESUMO,
        "conta": "Ativo Total",
        "is_dre": False,
        "is_wide": True,
        "description": "Total de ativos das instituições",
    },
    "carteira_credito": {
        "label": "Operações de Crédito",
        "icon": "💳",
        "relatorio": RELATORIO_RESUMO,
        "conta": "Carteira de Crédito",
        "is_dre": False,
        "is_wide": True,
        "description": "Carteira de crédito total",
    },
    "captacoes": {
        "label": "Captações",
        "icon": "💰",
        "relatorio": RELATORIO_RESUMO,
        "conta": "Captações",
        "is_dre": False,
        "is_wide": True,
        "description": "Total de captações (depósitos e funding)",
    },
    "patrimonio_liquido": {
        "label": "Patrimônio Líquido",
        "icon": "🛡️",
        "relatorio": RELATORIO_RESUMO,
        "conta": "Patrimônio Líquido",
        "is_dre": False,
        "is_wide": True,
        "description": "Capital próprio das instituições",
    },
}

# ─────────────────────────────────────────────
# MODULE 2: RESULTADO — Variable definitions
# DRE (Relatório 4) — Anualizado (soma 4 trimestres)
# ─────────────────────────────────────────────
RELATORIO_DRE = 4

MODULO2_VARS = {
    "resultado_intermediacao": {
        "label": "Resultado de Intermediação Financeira",
        "icon": "🔄",
        "relatorio": RELATORIO_DRE,
        "conta": "Resultado de Intermediação Financeira \n(c) = (a) + (b)",
        "description": "Receitas menos despesas de intermediação",
    },
    "despesas_captacao": {
        "label": "Despesas de Captação",
        "icon": "💸",
        "relatorio": RELATORIO_DRE,
        "conta": "Despesas de Captação \n(b1)",
        "description": "Custo de captação de recursos",
    },
    "rendas_tarifas": {
        "label": "Tarifas Bancárias",
        "icon": "🏷️",
        "relatorio": RELATORIO_DRE,
        "conta": "Rendas de Tarifas Bancárias \n(d2)",
        "description": "Rendas de tarifas bancárias",
    },
    "rendas_servicos": {
        "label": "Rendas de Serviços",
        "icon": "🎯",
        "relatorio": RELATORIO_DRE,
        "conta": "Rendas de Prestação de Serviços \n(d1)",
        "description": "Rendas de prestação de serviços",
    },
    "despesas_pessoal": {
        "label": "Despesas de Pessoal",
        "icon": "👥",
        "relatorio": RELATORIO_DRE,
        "conta": "Despesas de Pessoal \n(d3)",
        "description": "Despesas com funcionários",
    },
    "despesas_admin": {
        "label": "Despesas Administrativas",
        "icon": "🏛️",
        "relatorio": RELATORIO_DRE,
        "conta": "Despesas Administrativas \n(d4)",
        "description": "Despesas administrativas gerais",
    },
    "lucro_liquido": {
        "label": "Lucro Líquido",
        "icon": "✨",
        "relatorio": RELATORIO_DRE,
        "conta": "Lucro Líquido \n(j) = (g) + (h) + (i)",
        "description": "Resultado líquido do período",
    },
}

# ─────────────────────────────────────────────
# MODULE 3: CRÉDITO PF — Variable definitions
# Relatório 11, TipoInstituicao=2
# Filtro: Grupo = modalidade, NomeColuna = "Total"
# ─────────────────────────────────────────────
TIPO_INST_CREDITO = 2  # TipoInstituicao=2 for credit reports

MODULO3_VARS = {
    "total_pf": {
        "label": "Total Pessoa Física",
        "icon": "👥",
        "grupo": "Total da Carteira de Pessoa Física",
        "description": "Carteira total de crédito PF",
    },
    "consignado": {
        "label": "Consignado",
        "icon": "📋",
        "grupo": "Empréstimo com Consignação em Folha",
        "description": "Crédito consignado em folha",
    },
    "pessoal": {
        "label": "Empréstimo Pessoal",
        "icon": "💵",
        "grupo": "Empréstimo sem Consignação em Folha",
        "description": "Crédito pessoal sem consignação",
    },
    "habitacao": {
        "label": "Habitação",
        "icon": "🏠",
        "grupo": "Habitação",
        "description": "Crédito imobiliário",
    },
    "veiculos": {
        "label": "Veículos",
        "icon": "🚗",
        "grupo": "Veículos",
        "description": "Financiamento de veículos",
    },
    "cartao": {
        "label": "Cartão de Crédito",
        "icon": "💳",
        "grupo": "Cartão de Crédito",
        "description": "Saldo de cartão de crédito",
    },
    "rural_pf": {
        "label": "Rural e Agroindustrial",
        "icon": "🌾",
        "grupo": "Rural e Agroindustrial",
        "description": "Crédito rural PF",
    },
    "outros_pf": {
        "label": "Outros Créditos",
        "icon": "📦",
        "grupo": "Outros Créditos",
        "description": "Outras modalidades de crédito PF",
    },
}

# ─────────────────────────────────────────────
# MODULE 4: CRÉDITO PJ — Variable definitions
# ─────────────────────────────────────────────
MODULO4_VARS = {
    "total_pj": {
        "label": "Total Pessoa Jurídica",
        "icon": "🏢",
        "grupo": "Total da Carteira de Pessoa Jurídica",
        "description": "Carteira total de crédito PJ",
    },
    "capital_giro": {
        "label": "Capital de Giro",
        "icon": "🔄",
        "grupo": "Capital de Giro",
        "description": "Crédito para capital de giro",
    },
    "investimento": {
        "label": "Investimento",
        "icon": "📈",
        "grupo": "Investimento",
        "description": "Financiamento de investimento",
    },
    "recebiveis": {
        "label": "Operações com Recebíveis",
        "icon": "📄",
        "grupo": "Operações com Recebíveis",
        "description": "Antecipação de recebíveis",
    },
    "conta_garantida": {
        "label": "Conta Garantida",
        "icon": "📝",
        "grupo": "Cheque Especial e Conta Garantida",
        "description": "Cheque especial e conta garantida",
    },
    "habitacional_pj": {
        "label": "Habitacional",
        "icon": "🏗️",
        "grupo": "Habitacional",
        "description": "Crédito habitacional PJ",
    },
    "infraestrutura": {
        "label": "Infraestrutura / Projeto",
        "icon": "🏭",
        "grupo": "Financiamento de Infraestrutura/Desenvolvimento/Projeto e Outros Créditos",
        "description": "Infra, desenvolvimento e outros",
    },
    "comex": {
        "label": "Comércio Exterior",
        "icon": "🌎",
        "grupo": "Comércio Exterior",
        "description": "Financiamento de comércio exterior",
    },
    "rural_pj": {
        "label": "Rural e Agroindustrial",
        "icon": "🌾",
        "grupo": "Rural e Agroindustrial",
        "description": "Crédito rural PJ",
    },
    "outros_pj": {
        "label": "Outros Créditos",
        "icon": "📦",
        "grupo": "Outros Créditos",
        "description": "Outras modalidades de crédito PJ",
    },
}

# ─────────────────────────────────────────────
# MODULE 6: CRÉDITO POR REGIÃO — Variable defs
# Relatório 9, TipoInstituicao=1
# Filtro: NomeColuna = região (sem Grupo)
# ─────────────────────────────────────────────
MODULO6_VARS = {
    "sudeste": {
        "label": "Sudeste",
        "icon": "🏙️",
        "conta": "Sudeste",
        "description": "Carteira de crédito na região Sudeste",
    },
    "centro_oeste": {
        "label": "Centro-Oeste",
        "icon": "🌾",
        "conta": "Centro-oeste",
        "description": "Carteira de crédito na região Centro-Oeste",
    },
    "nordeste": {
        "label": "Nordeste",
        "icon": "☀️",
        "conta": "Nordeste",
        "description": "Carteira de crédito na região Nordeste",
    },
    "norte": {
        "label": "Norte",
        "icon": "🌿",
        "conta": "Norte",
        "description": "Carteira de crédito na região Norte",
    },
    "sul": {
        "label": "Sul",
        "icon": "❄️",
        "conta": "Sul",
        "description": "Carteira de crédito na região Sul",
    },
}

# ─────────────────────────────────────────────
# FORMATAÇÃO
# ─────────────────────────────────────────────
def format_brl(value: float) -> str:
    """Format value (raw, e.g. 1e9 = R$ 1 bi) to readable BRL string."""
    if abs(value) >= 1e9:
        return f"R$ {value / 1e9:,.1f} bi"
    elif abs(value) >= 1e6:
        return f"R$ {value / 1e6:,.1f} mi"
    elif abs(value) >= 1e3:
        return f"R$ {value / 1e3:,.1f} mil"
    else:
        return f"R$ {value:,.0f}"


def format_pct(value: float) -> str:
    """Format percentage."""
    return f"{value:.1f}%"
