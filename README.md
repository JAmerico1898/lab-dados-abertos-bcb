# Laboratório de Dados Públicos — BCB

Portal interativo para exploração e visualização de dados do **Banco Central do Brasil**, construído com [Streamlit](https://streamlit.io/) e a biblioteca [python-bcb](https://github.com/wilsonfreitas/python-bcb).

Desenvolvido para fins educacionais — **COPPEAD/UFRJ**.

---

## Visão Geral

O aplicativo consulta as APIs oficiais do BCB (IF.data e TaxaJuros) e apresenta os dados em visualizações interativas: treemaps, barras horizontais, cartogramas e tabelas estilizadas.

### Módulos

| # | Módulo | Visualização | Descrição |
|---|--------|-------------|-----------|
| 1 | Ativos e Passivos | Treemap | Ativo Total, Operações de Crédito, Captações, PL |
| 2 | Resultado | Treemap | Intermediação Financeira, Despesas, Lucro Líquido |
| 3 | Crédito Pessoa Física | Treemap | Consignado, Habitação, Veículos, Cartão e mais |
| 4 | Crédito Pessoa Jurídica | Treemap | Capital de Giro, Investimento, Recebíveis e mais |
| 5 | Taxas de Juros | Ranking + Gráficos | Rankings de taxas por modalidade de crédito |
| 6 | Crédito por Região | Treemap | Carteira de crédito por região geográfica |
| 7 | Índices Financeiros | Barras Horizontais | Basileia, ROI, Eficiência, Alavancagem e mais |
| 8 | Cartograma | Dorling SVG | Mapa do Brasil com círculos proporcionais ao crédito |
| — | Feedback | Formulário | Sugestões, dúvidas e bug reports |

---

## Estrutura do Projeto

```
laboratorio_dados_publicos/
├── app.py                  # Hub central + router
├── config.py               # Constantes, variáveis, relatórios
├── data_utils.py           # Funções de fetch, cache, extração
├── ui_components.py        # CSS global, componentes visuais
├── requirements.txt        # Dependências Python
├── .gitignore
├── .streamlit/
│   └── secrets.toml        # (opcional) chaves Pushover para feedback
└── pages/
    ├── __init__.py
    ├── modulo_1_ativos_passivos.py
    ├── modulo_2_resultado.py
    ├── modulo_3_credito_pf.py
    ├── modulo_4_credito_pj.py
    ├── modulo_5_taxas_juros.py
    ├── modulo_6_credito_regiao.py
    ├── modulo_7_indices.py
    ├── modulo_8_cartograma.py
    └── modulo_feedback.py
```

---

## Instalação Local

### Pré-requisitos

- Python 3.10+
- pip

### Passos

```bash
# 1. Clone o repositório
git clone https://github.com/SEU_USUARIO/laboratorio-dados-publicos.git
cd laboratorio-dados-publicos

# 2. Crie um ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Execute o app
streamlit run app.py
```

O app abrirá em `http://localhost:8501`.

---

## Deploy na Streamlit Cloud

1. Faça push do repositório para o GitHub.
2. Acesse [share.streamlit.io](https://share.streamlit.io/).
3. Conecte o repositório e selecione `app.py` como arquivo principal.
4. O deploy é automático a cada push na branch principal.

### Configuração de Secrets (opcional)

Para habilitar notificações no módulo de Feedback, configure em **Settings > Secrets** na Streamlit Cloud:

```toml
PUSHOVER_API_TOKEN = "sua_chave_api"
PUSHOVER_USER_KEY = "sua_chave_usuario"
```

---

## Fontes de Dados

Todos os dados são obtidos em tempo real das APIs oficiais do Banco Central:

- **IF.data** — `https://olinda.bcb.gov.br/olinda/servico/IFDATA/`
  - Relatório 1: Resumo
  - Relatório 2: Ativo
  - Relatório 3: Passivo
  - Relatório 4: Demonstração de Resultado
  - Relatório 9: Crédito por Região
  - Relatório 11: Crédito Pessoa Física
  - Relatório 13: Crédito Pessoa Jurídica
- **TaxaJuros** — `https://olinda.bcb.gov.br/olinda/servico/TaxaJuros/`
  - TaxasJurosDiariaPorInicioPeriodo
  - TaxasJurosMensalPorMes

Os dados são cacheados por 2 horas (`st.cache_data`, TTL=7200s) para reduzir chamadas à API.

---

## Tecnologias

- **Streamlit** — framework web para apps de dados
- **python-bcb** — wrapper oficial das APIs do Banco Central
- **Plotly** — treemaps, barras horizontais, gráficos de dispersão
- **Pandas** — manipulação e análise de dados
- **openpyxl** — exportação para Excel

---

## Performance

A primeira carga de cada módulo pode levar alguns segundos devido às consultas à API do BCB. Após o carregamento inicial, os dados ficam em cache. Para melhorar a performance:

- Os módulos carregam dados sob demanda (apenas quando acessados).
- O filtro padrão é S1+S2 (menos instituições = menos dados).
- O cache expira a cada 2 horas.

---

## Licença

Projeto educacional — COPPEAD/UFRJ. Dados públicos do Banco Central do Brasil.
