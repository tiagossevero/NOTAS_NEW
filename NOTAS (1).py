"""
================================================================================
SISTEMA NOTAS - Análise de Notas Fiscais Eletrônicas (NFe/NFCe)
================================================================================
Sistema para análise de notas fiscais eletrônicas com comparação setorial
Autor: SEF/SC - Secretaria de Estado da Fazenda de Santa Catarina
================================================================================
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import re
from typing import Optional, Dict, Any, List, Tuple
import io
import ssl
import warnings
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# Configurações SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="NOTAS - Análise de NFe/NFCe",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS para reduzir espaço no topo da página
st.markdown("""
<style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
    }
    header[data-testid="stHeader"] {
        height: 0;
    }
    .stMainBlockContainer {
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# CONFIGURAÇÃO DO BANCO DE DADOS
# =============================================================================
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
IMPALA_USER = st.secrets["impala_credentials"]["user"]
IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]

@st.cache_resource
def get_impala_engine():
    """Cria e retorna engine Impala."""
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}',
            connect_args={
                'user': IMPALA_USER,
                'password': IMPALA_PASSWORD,
                'auth_mechanism': 'LDAP',
                'use_ssl': True
            }
        )
        return engine
    except Exception as e:
        st.error(f"❌ Erro ao criar engine Impala: {e}")
        return None

def executar_query(query: str) -> pd.DataFrame:
    """Executa query no Impala e retorna DataFrame."""
    engine = get_impala_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Erro na query: {e}")
        return pd.DataFrame()

def executar_query_thread_safe(query: str) -> pd.DataFrame:
    """Executa query no Impala de forma thread-safe (sem usar st.error)."""
    engine = get_impala_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Erro na query: {e}")  # Log para debug
        return pd.DataFrame()

@st.cache_data(ttl=600, show_spinner=False)
def executar_query_cached(query: str, _cache_key: str = None) -> pd.DataFrame:
    """
    Executa query no Impala COM CACHE de 10 minutos.
    Use _cache_key para diferenciar queries com mesmo SQL mas contextos diferentes.
    """
    engine = get_impala_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        print(f"Erro na query: {e}")
        return pd.DataFrame()

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def limpar_cnpj(cnpj: str) -> str:
    """Remove formatação do CNPJ."""
    if cnpj:
        return re.sub(r'[^0-9]', '', str(cnpj))
    return ""

def limpar_ie(ie: str) -> str:
    """Remove formatação da Inscrição Estadual (pontos, hífens, etc.)."""
    if ie:
        return re.sub(r'[^0-9]', '', str(ie))
    return ""

def formatar_cnpj(cnpj: str) -> str:
    """Formata CNPJ para exibição."""
    cnpj = limpar_cnpj(cnpj)
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"
    return cnpj

def formatar_ie(ie: str) -> str:
    """Formata IE para exibição no formato XX.XXX.XXX-X."""
    if ie:
        ie = limpar_ie(ie)
        if len(ie) == 9:
            return f"{ie[:2]}.{ie[2:5]}.{ie[5:8]}-{ie[8]}"
    return ie or "-"

def formatar_moeda(valor: float) -> str:
    """Formata valor como moeda brasileira."""
    if pd.isna(valor) or valor is None:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_percentual(valor: float) -> str:
    """Formata valor como percentual."""
    if pd.isna(valor) or valor is None:
        return "0,00%"
    return f"{valor:.2f}%".replace(".", ",")

def formatar_numero(valor: float) -> str:
    """Formata número com separador de milhar."""
    if pd.isna(valor) or valor is None:
        return "0"
    return f"{valor:,.0f}".replace(",", ".")

# =============================================================================
# CONFIGURAÇÕES DE COLUNAS PARA TABELAS (ordenação correta + formatação)
# =============================================================================

def col_moeda(label: str, help_text: str = None):
    """Coluna de valor monetário com formatação BR e ordenação correta."""
    return st.column_config.NumberColumn(
        label,
        help=help_text,
        format="R$ %.2f"
    )

def col_numero(label: str, help_text: str = None):
    """Coluna numérica inteira com ordenação correta."""
    return st.column_config.NumberColumn(
        label,
        help=help_text,
        format="%d"
    )

def col_percentual(label: str, help_text: str = None):
    """Coluna de percentual com ordenação correta."""
    return st.column_config.NumberColumn(
        label,
        help=help_text,
        format="%.2f%%"
    )

def col_barra_valor(label: str, max_value: float = None, help_text: str = None):
    """Coluna com barra de progresso colorida (verde=maior, vermelho=menor)."""
    return st.column_config.ProgressColumn(
        label,
        help=help_text,
        format="R$ %.2f",
        min_value=0,
        max_value=float(max_value) if max_value is not None else None
    )

def col_barra_qtd(label: str, max_value: float = None, help_text: str = None):
    """Coluna com barra de progresso para quantidades."""
    return st.column_config.ProgressColumn(
        label,
        help=help_text,
        format="%d",
        min_value=0,
        max_value=float(max_value) if max_value is not None else None
    )

def col_barra_pct(label: str, help_text: str = None):
    """Coluna com barra de progresso para percentuais (0-100)."""
    return st.column_config.ProgressColumn(
        label,
        help=help_text,
        format="%.1f%%",
        min_value=0,
        max_value=100.0
    )

def calcular_periodo_default() -> Tuple[int, int]:
    """Retorna período padrão: últimos 6 meses."""
    hoje = date.today()
    fim = hoje.year * 100 + hoje.month
    inicio_data = hoje - relativedelta(months=5)
    inicio = inicio_data.year * 100 + inicio_data.month
    return inicio, fim

def periodo_para_texto(periodo: int) -> str:
    """Converte período YYYYMM para MM/YYYY."""
    ano = periodo // 100
    mes = periodo % 100
    return f"{mes:02d}/{ano}"

# =============================================================================
# QUERIES SQL
# =============================================================================

class NotasQueries:
    """Repositório de queries SQL para o sistema NOTAS."""
    
    # =========================================================================
    # NOTA: Impala suporta acesso a arrays usando JOIN implícito com o array
    # Sintaxe: SELECT ... FROM tabela t, t.caminho.para.array arr
    # Isso permite acessar todos os itens do array como linhas individuais
    # Referência: https://impala.apache.org/docs/build/html/topics/impala_complex_types.html
    # =========================================================================
    
    @staticmethod
    def get_cadastro_query(ie: str = None, cnpj: str = None) -> str:
        """Query para dados cadastrais do contribuinte."""
        if ie:
            where_clause = f"TRIM(oc.nu_ie) = '{ie}'"
        else:
            where_clause = f"REGEXP_REPLACE(TRIM(CAST(oc.nu_cnpj AS STRING)), '[^0-9]', '') = '{cnpj}'"
        
        return f"""
        SELECT
            TRIM(oc.nu_ie) AS inscricao_estadual,
            oc.nu_cnpj AS cnpj,
            oc.nu_cnpj_grupo AS cnpj_grupo,
            oc.nm_razao_social AS razao_social,
            oc.nm_fantasia AS nome_fantasia,
            oc.cd_sit_cadastral AS situacao_cadastral_cod,
            oc.nm_sit_cadastral AS situacao_cadastral_desc,
            oc.dt_sit_cadastral AS data_situacao_cadastral,
            LPAD(CAST(oc.cd_cnae AS STRING), 7, '0') AS cnae,
            oc.de_classe AS descricao_cnae,
            oc.cd_secao AS secao_cnae,
            oc.de_secao AS descricao_secao,
            oc.cd_reg_apuracao AS regime_apuracao_cod,
            oc.nm_reg_apuracao AS regime_apuracao_desc,
            oc.cd_tipo_contribuinte AS tipo_contribuinte_cod,
            oc.nm_tipo_contribuinte AS tipo_contribuinte_desc,
            oc.nm_enq_empresa AS enquadramento_empresa,
            oc.nm_natureza_juridica AS natureza_juridica,
            oc.dt_constituicao_empresa AS data_constituicao,
            oc.dt_inicio_icms AS data_inicio_icms,
            oc.sn_simples_nacional_rfb AS flag_simples_nacional,
            oc.nm_usefi AS usefi,
            oc.nm_gerfe AS gerfe,
            oc.nm_munic AS municipio,
            oc.cd_uf AS uf,
            oc.cd_cep AS cep,
            oc.nm_logradouro AS logradouro,
            oc.nu_logradouro AS numero,
            oc.nm_bairro AS bairro,
            oc.nu_telefone AS telefone,
            oc.nm_email AS email,
            oc.nm_contador AS nome_contador,
            oc.nu_cpf_cnpj_contador AS cpf_cnpj_contador,
            oc.nu_crc_contador AS crc_contador,
            oc.qt_socios AS qtd_socios,
            oc.qt_socios_ativos AS qtd_socios_ativos,
            oc.dt_ult_atualiz AS data_ultima_atualizacao
        FROM usr_sat_ods.vw_ods_contrib oc
        WHERE {where_clause}
        LIMIT 1
        """

    @staticmethod
    def get_nfe_emitidas_resumo(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para resumo de NFe emitidas por período - usando totais da nota."""
        return f"""
        SELECT
            CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0')) AS periodo,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
            COUNT(*) AS qtd_itens
        FROM nfe.nfe a
        WHERE a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
          AND a.situacao = 1
          AND a.procnfe.nfe.infnfe.ide.tpnf = 1
          AND a.procnfe.nfe.infnfe.ide.finnfe = 1
          AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
        GROUP BY CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0'))
        ORDER BY periodo
        """

    @staticmethod
    def get_nfe_recebidas_resumo(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para resumo de NFe recebidas por período - usando totais da nota."""
        return f"""
        SELECT
            CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0')) AS periodo,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
            COUNT(*) AS qtd_itens
        FROM nfe.nfe a
        WHERE a.procnfe.nfe.infnfe.dest.cnpj = '{cnpj}'
          AND a.situacao = 1
          AND a.procnfe.nfe.infnfe.ide.finnfe = 1
          AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
        GROUP BY CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0'))
        ORDER BY periodo
        """

    @staticmethod
    def get_nfce_resumo(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para resumo de NFCe por período - usando totais da nota."""
        return f"""
        SELECT
            CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0')) AS periodo,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
            COUNT(*) AS qtd_itens
        FROM nfce.nfce a
        WHERE a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
          AND a.situacao = 1
          AND a.procnfe.nfe.infnfe.ide.finnfe = 1
          AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
        GROUP BY CONCAT(CAST(a.ano_emissao AS STRING), LPAD(CAST(a.mes_emissao AS STRING), 2, '0'))
        ORDER BY periodo
        """

    @staticmethod
    def get_top_clientes(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 10) -> str:
        """Query para top clientes (NFe emitidas) - sem EXPLODE."""
        return f"""
        SELECT
            a.procnfe.nfe.infnfe.dest.cnpj AS cnpj_cliente,
            a.procnfe.nfe.infnfe.dest.xnome AS razao_social,
            a.procnfe.nfe.infnfe.dest.enderdest.uf AS uf_cliente,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
            COUNT(*) AS qtd_itens
        FROM nfe.nfe a
        WHERE a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
          AND a.situacao = 1
          AND a.procnfe.nfe.infnfe.ide.tpnf = 1
          AND a.procnfe.nfe.infnfe.ide.finnfe = 1
          AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
          AND a.procnfe.nfe.infnfe.dest.cnpj IS NOT NULL
          AND TRIM(a.procnfe.nfe.infnfe.dest.cnpj) != ''
        GROUP BY 
            a.procnfe.nfe.infnfe.dest.cnpj,
            a.procnfe.nfe.infnfe.dest.xnome,
            a.procnfe.nfe.infnfe.dest.enderdest.uf
        ORDER BY valor_total DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_top_fornecedores(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 10) -> str:
        """Query para top fornecedores (NFe recebidas) - sem EXPLODE."""
        return f"""
        SELECT
            a.procnfe.nfe.infnfe.emit.cnpj AS cnpj_fornecedor,
            a.procnfe.nfe.infnfe.emit.xnome AS razao_social,
            a.procnfe.nfe.infnfe.emit.enderemit.uf AS uf_fornecedor,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
            COUNT(*) AS qtd_itens
        FROM nfe.nfe a
        WHERE a.procnfe.nfe.infnfe.dest.cnpj = '{cnpj}'
          AND a.situacao = 1
          AND a.procnfe.nfe.infnfe.ide.finnfe = 1
          AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
        GROUP BY 
            a.procnfe.nfe.infnfe.emit.cnpj,
            a.procnfe.nfe.infnfe.emit.xnome,
            a.procnfe.nfe.infnfe.emit.enderemit.uf
        ORDER BY valor_total DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_top_ncm_nfe(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 10) -> str:
        """Query para top NCM em NFe emitidas usando JOIN com array e tabela de descrição."""
        return f"""
        SELECT 
            n.ncm,
            COALESCE(t.descricao, 'Descrição não encontrada') AS descricao_ncm,
            n.qtd_notas,
            n.valor_total,
            n.qtd_itens
        FROM (
            SELECT 
                det.item.prod.ncm AS ncm,
                COUNT(DISTINCT a.chave) AS qtd_notas,
                SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
                COUNT(*) AS qtd_itens
            FROM 
                nfe.nfe a,
                a.procnfe.nfe.infnfe.det det
            WHERE 
                a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
                AND a.situacao = 1
                AND a.procnfe.nfe.infnfe.ide.tpnf = 1
                AND a.procnfe.nfe.infnfe.ide.finnfe = 1
                AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
                AND det.item.prod.ncm IS NOT NULL
            GROUP BY det.item.prod.ncm
            ORDER BY valor_total DESC
            LIMIT {limit}
        ) n
        LEFT JOIN niat.tabela_ncm t ON n.ncm = t.ncm
        ORDER BY n.valor_total DESC
        """

    @staticmethod
    def get_top_ncm_nfce(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 10) -> str:
        """Query para top NCM em NFCe usando JOIN com array e tabela de descrição."""
        return f"""
        SELECT 
            n.ncm,
            COALESCE(t.descricao, 'Descrição não encontrada') AS descricao_ncm,
            n.qtd_notas,
            n.valor_total,
            n.qtd_itens
        FROM (
            SELECT 
                det.item.prod.ncm AS ncm,
                COUNT(DISTINCT a.chave) AS qtd_notas,
                SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
                COUNT(*) AS qtd_itens
            FROM 
                nfce.nfce a,
                a.procnfe.nfe.infnfe.det det
            WHERE 
                a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
                AND a.situacao = 1
                AND a.procnfe.nfe.infnfe.ide.finnfe = 1
                AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
                AND det.item.prod.ncm IS NOT NULL
            GROUP BY det.item.prod.ncm
            ORDER BY valor_total DESC
            LIMIT {limit}
        ) n
        LEFT JOIN niat.tabela_ncm t ON n.ncm = t.ncm
        ORDER BY n.valor_total DESC
        """

    @staticmethod
    def get_cfop_nfe(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para distribuição por CFOP usando JOIN com array e tabela de descrição."""
        return f"""
        SELECT 
            c.cfop,
            COALESCE(t.descricaocfop, 'Descrição não encontrada') AS descricao_cfop,
            COALESCE(t.eous, '') AS entrada_saida,
            COALESCE(t.`local`, '') AS local_operacao,
            COALESCE(t.indcom, '') AS tipo_operacao,
            c.qtd_notas,
            c.valor_total,
            c.qtd_itens,
            c.valor_icms
        FROM (
            SELECT 
                det.item.prod.cfop AS cfop,
                COUNT(DISTINCT a.chave) AS qtd_notas,
                SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
                COUNT(*) AS qtd_itens,
                SUM(COALESCE(det.item.imposto.icms.resumo.vicms, 0)) AS valor_icms
            FROM 
                nfe.nfe a,
                a.procnfe.nfe.infnfe.det det
            WHERE 
                a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
                AND a.situacao = 1
                AND a.procnfe.nfe.infnfe.ide.tpnf = 1
                AND a.procnfe.nfe.infnfe.ide.finnfe = 1
                AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
                AND det.item.prod.cfop IS NOT NULL
            GROUP BY det.item.prod.cfop
        ) c
        LEFT JOIN niat.tabela_cfop t ON CAST(c.cfop AS STRING) = CAST(t.cfop AS STRING)
        ORDER BY c.valor_total DESC
        """

    @staticmethod
    def get_top_produtos_nfe(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 20) -> str:
        """Query para top produtos em NFe usando JOIN com array."""
        return f"""
        SELECT 
            det.item.prod.xprod AS descricao,
            det.item.prod.ncm AS ncm,
            det.item.prod.cprod AS codigo,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
            SUM(COALESCE(det.item.prod.qcom, 0)) AS qtd_vendida,
            COUNT(*) AS qtd_itens
        FROM 
            nfe.nfe a,
            a.procnfe.nfe.infnfe.det det
        WHERE 
            a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
            AND a.situacao = 1
            AND a.procnfe.nfe.infnfe.ide.tpnf = 1
            AND a.procnfe.nfe.infnfe.ide.finnfe = 1
            AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
            AND det.item.prod.xprod IS NOT NULL
        GROUP BY 
            det.item.prod.xprod,
            det.item.prod.ncm,
            det.item.prod.cprod
        ORDER BY valor_total DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_top_produtos_nfce(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 20) -> str:
        """Query para top produtos em NFCe usando JOIN com array."""
        return f"""
        SELECT 
            det.item.prod.xprod AS descricao,
            det.item.prod.ncm AS ncm,
            det.item.prod.cprod AS codigo,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
            SUM(COALESCE(det.item.prod.qcom, 0)) AS qtd_vendida,
            COUNT(*) AS qtd_itens
        FROM 
            nfce.nfce a,
            a.procnfe.nfe.infnfe.det det
        WHERE 
            a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
            AND a.situacao = 1
            AND a.procnfe.nfe.infnfe.ide.finnfe = 1
            AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
            AND det.item.prod.xprod IS NOT NULL
        GROUP BY 
            det.item.prod.xprod,
            det.item.prod.ncm,
            det.item.prod.cprod
        ORDER BY valor_total DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_top_ncm_entrada(cnpj: str, periodo_inicio: int, periodo_fim: int, limit: int = 10) -> str:
        """Query para top NCM em NFe RECEBIDAS (entradas) usando JOIN com array."""
        return f"""
        SELECT 
            n.ncm,
            COALESCE(t.descricao, 'Descrição não encontrada') AS descricao_ncm,
            n.qtd_notas,
            n.valor_total,
            n.qtd_itens
        FROM (
            SELECT 
                det.item.prod.ncm AS ncm,
                COUNT(DISTINCT a.chave) AS qtd_notas,
                SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
                COUNT(*) AS qtd_itens
            FROM 
                nfe.nfe a,
                a.procnfe.nfe.infnfe.det det
            WHERE 
                a.procnfe.nfe.infnfe.dest.cnpj = '{cnpj}'
                AND a.situacao = 1
                AND a.procnfe.nfe.infnfe.ide.finnfe = 1
                AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
                AND det.item.prod.ncm IS NOT NULL
            GROUP BY det.item.prod.ncm
            ORDER BY valor_total DESC
            LIMIT {limit}
        ) n
        LEFT JOIN niat.tabela_ncm t ON n.ncm = t.ncm
        ORDER BY n.valor_total DESC
        """

    @staticmethod
    def get_cfop_entrada(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para distribuição por CFOP em NFe RECEBIDAS (entradas)."""
        return f"""
        SELECT 
            c.cfop,
            COALESCE(t.descricaocfop, 'Descrição não encontrada') AS descricao_cfop,
            COALESCE(t.eous, '') AS entrada_saida,
            COALESCE(t.`local`, '') AS local_operacao,
            COALESCE(t.indcom, '') AS tipo_operacao,
            c.qtd_notas,
            c.valor_total,
            c.qtd_itens,
            c.valor_icms
        FROM (
            SELECT 
                det.item.prod.cfop AS cfop,
                COUNT(DISTINCT a.chave) AS qtd_notas,
                SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_total,
                COUNT(*) AS qtd_itens,
                SUM(COALESCE(det.item.imposto.icms.resumo.vicms, 0)) AS valor_icms
            FROM 
                nfe.nfe a,
                a.procnfe.nfe.infnfe.det det
            WHERE 
                a.procnfe.nfe.infnfe.dest.cnpj = '{cnpj}'
                AND a.situacao = 1
                AND a.procnfe.nfe.infnfe.ide.finnfe = 1
                AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
                AND det.item.prod.cfop IS NOT NULL
            GROUP BY det.item.prod.cfop
        ) c
        LEFT JOIN niat.tabela_cfop t ON CAST(c.cfop AS STRING) = CAST(t.cfop AS STRING)
        ORDER BY c.valor_total DESC
        """

    @staticmethod
    def get_faturamento_dime(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para faturamento DIME (regime normal)."""
        return f"""
        SELECT
            CAST(nu_per_ref AS INT) AS periodo,
            COALESCE(CAST(VL_FATURAMENTO AS DOUBLE), 0) AS faturamento,
            COALESCE(CAST(VL_RECEITA_BRUTA AS DOUBLE), 0) AS receita_bruta,
            COALESCE(CAST(VL_TOT_CRED AS DOUBLE), 0) AS total_creditos,
            COALESCE(CAST(VL_TOT_DEB AS DOUBLE), 0) AS total_debitos,
            COALESCE(CAST(VL_DEB_RECOLHER AS DOUBLE), 0) AS debito_recolher
        FROM usr_sat_ods.ods_decl_dime_raw
        WHERE REGEXP_REPLACE(TRIM(CAST(NU_CNPJ AS STRING)), '[^0-9]', '') = '{cnpj}'
          AND CAST(nu_per_ref AS INT) BETWEEN {periodo_inicio} AND {periodo_fim}
        ORDER BY nu_per_ref
        """

    @staticmethod
    def get_faturamento_pgdas(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para faturamento PGDAS (Simples Nacional)."""
        return f"""
        SELECT
            CAST(nu_per_ref AS INT) AS periodo,
            COALESCE(CAST(vl_rec_bruta_estab AS DOUBLE), 0) AS receita_bruta
        FROM usr_sat_ods.sna_pgdasd_estabelecimento_raw
        WHERE REGEXP_REPLACE(TRIM(CAST(nu_cnpj AS STRING)), '[^0-9]', '') = '{cnpj}'
          AND CAST(nu_per_ref AS INT) BETWEEN {periodo_inicio} AND {periodo_fim}
        ORDER BY nu_per_ref
        """

    @staticmethod
    def get_setor_stats(cnae: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para estatísticas do setor (empresas do mesmo CNAE) - usando totais da nota."""
        return f"""
        WITH empresas_setor AS (
            SELECT DISTINCT 
                oc.nu_cnpj AS cnpj,
                oc.nm_razao_social AS razao_social
            FROM usr_sat_ods.vw_ods_contrib oc
            WHERE LPAD(CAST(oc.cd_cnae AS STRING), 7, '0') = '{cnae}'
              AND oc.nm_sit_cadastral = 'ATIVO'
              AND oc.cd_uf = 'SC'
        ),
        vendas_setor AS (
            SELECT
                a.procnfe.nfe.infnfe.emit.cnpj AS cnpj,
                SUM(COALESCE(a.procnfe.nfe.infnfe.total.icmstot.vnf, 0)) AS valor_total,
                COUNT(DISTINCT a.chave) AS qtd_notas
            FROM nfe.nfe a
            INNER JOIN empresas_setor e ON a.procnfe.nfe.infnfe.emit.cnpj = e.cnpj
            WHERE a.situacao = 1
              AND a.procnfe.nfe.infnfe.ide.tpnf = 1
              AND a.procnfe.nfe.infnfe.ide.finnfe = 1
              AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
            GROUP BY a.procnfe.nfe.infnfe.emit.cnpj
        )
        SELECT
            COUNT(*) AS qtd_empresas,
            AVG(valor_total) AS media_faturamento,
            STDDEV(valor_total) AS desvio_faturamento,
            MIN(valor_total) AS min_faturamento,
            MAX(valor_total) AS max_faturamento,
            APPX_MEDIAN(valor_total) AS mediana_faturamento,
            AVG(qtd_notas) AS media_notas
        FROM vendas_setor
        WHERE valor_total > 0
        """

    @staticmethod
    def get_tributacao_nfe(cnpj: str, periodo_inicio: int, periodo_fim: int) -> str:
        """Query para análise de tributação usando JOIN com array."""
        return f"""
        SELECT 
            COALESCE(det.item.imposto.icms.resumo.cst, 
                     CAST(det.item.imposto.icms.resumo.csosn AS STRING)) AS cst,
            det.item.imposto.icms.resumo.orig AS origem,
            det.item.imposto.icms.resumo.grupotributacao AS grupo_tributacao,
            COUNT(*) AS qtd_itens,
            COUNT(DISTINCT a.chave) AS qtd_notas,
            SUM(COALESCE(det.item.prod.vprod, 0)) AS valor_produtos,
            SUM(COALESCE(det.item.imposto.icms.resumo.vbc, 0)) AS base_calculo_total,
            SUM(COALESCE(det.item.imposto.icms.resumo.vicms, 0)) AS icms_total,
            SUM(COALESCE(det.item.imposto.icms.resumo.vicmsdeson, 0)) AS icms_desonerado,
            AVG(COALESCE(det.item.imposto.icms.resumo.picms, 0)) AS aliquota_media
        FROM 
            nfe.nfe a,
            a.procnfe.nfe.infnfe.det det
        WHERE 
            a.procnfe.nfe.infnfe.emit.cnpj = '{cnpj}'
            AND a.situacao = 1
            AND a.procnfe.nfe.infnfe.ide.tpnf = 1
            AND a.procnfe.nfe.infnfe.ide.finnfe = 1
            AND (a.ano_emissao * 100 + a.mes_emissao) BETWEEN {periodo_inicio} AND {periodo_fim}
        GROUP BY 
            COALESCE(det.item.imposto.icms.resumo.cst, 
                     CAST(det.item.imposto.icms.resumo.csosn AS STRING)),
            det.item.imposto.icms.resumo.orig,
            det.item.imposto.icms.resumo.grupotributacao
        ORDER BY icms_total DESC
        """

    @staticmethod
    def get_ttd_empresa(ie: str) -> str:
        """Query para TTDs ativos da empresa."""
        return f"""
        SELECT
            cd_beneficio,
            de_beneficio,
            nu_per_ini_vigencia AS periodo_inicio,
            COALESCE(nu_per_fim_vigencia, 209912) AS periodo_fim,
            nm_estado_benef_acordo AS estado
        FROM usr_sat_ods.vw_ods_ttd
        WHERE TRIM(nu_ie) = '{ie}'
          AND sn_valido = 1
          AND nm_estado_benef_acordo = 'ATIVO'
        ORDER BY cd_beneficio
        """

    # =========================================================================
    # QUERIES ARGOS - Análise Setorial Avançada
    # =========================================================================
    
    @staticmethod
    def get_periodo_mais_recente_argos() -> str:
        """Query para buscar período mais recente disponível no ARGOS."""
        return """
        SELECT MAX(nu_per_ref) AS periodo_mais_recente
        FROM niat.argos_benchmark_setorial
        """
    
    @staticmethod
    def get_benchmark_setorial(cnae_classe: str, periodo: int) -> str:
        """Query para benchmark do setor no período.
        CNAE no ARGOS usa 5 dígitos (classe), não 7 (subclasse)."""
        # Pegar apenas os 5 primeiros dígitos do CNAE
        cnae_5dig = cnae_classe[:5] if len(cnae_classe) >= 5 else cnae_classe
        return f"""
        SELECT 
            cnae_classe,
            desc_cnae_classe,
            qtd_empresas_total,
            qtd_empresas_ativas,
            faturamento_total,
            icms_devido_total,
            aliq_efetiva_media,
            aliq_efetiva_mediana,
            aliq_efetiva_p25,
            aliq_efetiva_p75,
            aliq_efetiva_desvio AS aliq_desvio_padrao,
            aliq_coef_variacao
        FROM niat.argos_benchmark_setorial
        WHERE cnae_classe = '{cnae_5dig}'
          AND nu_per_ref = {periodo}
        """

    @staticmethod
    def get_empresa_vs_benchmark(cnpj: str, periodo: int) -> str:
        """Query para comparação da empresa vs benchmark do setor."""
        return f"""
        SELECT 
            nu_cnpj,
            nm_razao_social,
            cnae_classe,
            desc_cnae_classe,
            porte_empresa,
            vl_faturamento,
            icms_devido,
            icms_recolher,
            aliq_efetiva_empresa,
            aliq_setor_mediana,
            aliq_setor_p25,
            aliq_setor_p75,
            aliq_setor_desvio,
            indice_vs_mediana_setor,
            indice_vs_mediana_porte,
            status_vs_setor,
            empresas_no_setor,
            empresas_mesmo_porte
        FROM niat.argos_empresa_vs_benchmark
        WHERE nu_cnpj = '{cnpj}'
          AND nu_per_ref = {periodo}
        """

    @staticmethod
    def get_empresas_setor(cnae_classe: str, periodo: int, limit: int = 50) -> str:
        """Query para listar empresas do mesmo setor.
        CNAE no ARGOS usa 5 dígitos (classe)."""
        cnae_5dig = cnae_classe[:5] if len(cnae_classe) >= 5 else cnae_classe
        return f"""
        SELECT 
            nu_cnpj,
            nm_razao_social,
            porte_empresa,
            vl_faturamento,
            icms_devido,
            aliq_efetiva_empresa,
            indice_vs_mediana_setor,
            status_vs_setor
        FROM niat.argos_empresa_vs_benchmark
        WHERE cnae_classe = '{cnae_5dig}'
          AND nu_per_ref = {periodo}
          AND vl_faturamento > 0
        ORDER BY vl_faturamento DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_alertas_empresa(cnpj: str, periodo: int = None) -> str:
        """Query para alertas da empresa."""
        periodo_cond = f"AND nu_per_ref = {periodo}" if periodo else ""
        return f"""
        SELECT 
            nu_per_ref,
            tipo_alerta,
            severidade,
            score_risco,
            aliq_efetiva_empresa,
            aliq_setor_mediana,
            vl_faturamento,
            icms_devido
        FROM niat.argos_alertas_empresas
        WHERE nu_cnpj = '{cnpj}'
          {periodo_cond}
        ORDER BY nu_per_ref DESC, score_risco DESC
        """

    @staticmethod
    def get_alertas_setor(cnae_classe: str, periodo: int, limit: int = 20) -> str:
        """Query para alertas das empresas do setor.
        CNAE no ARGOS usa 5 dígitos (classe)."""
        cnae_5dig = cnae_classe[:5] if len(cnae_classe) >= 5 else cnae_classe
        return f"""
        SELECT 
            nu_cnpj,
            nm_razao_social,
            porte_empresa,
            tipo_alerta,
            severidade,
            score_risco,
            vl_faturamento
        FROM niat.argos_alertas_empresas
        WHERE cnae_classe = '{cnae_5dig}'
          AND nu_per_ref = {periodo}
        ORDER BY score_risco DESC
        LIMIT {limit}
        """

    @staticmethod
    def get_evolucao_setor(cnae_classe: str) -> str:
        """Query para evolução temporal do setor.
        CNAE no ARGOS usa 5 dígitos (classe)."""
        cnae_5dig = cnae_classe[:5] if len(cnae_classe) >= 5 else cnae_classe
        return f"""
        SELECT 
            nu_per_ref,
            aliq_mediana_media_8m,
            volatilidade_aliquota,
            categoria_volatilidade_temporal,
            tendencia_aliquota,
            variacao_pct_3m
        FROM niat.argos_evolucao_temporal_setor
        WHERE cnae_classe = '{cnae_5dig}'
        ORDER BY nu_per_ref
        """

    @staticmethod
    def get_benchmark_por_porte(cnae_classe: str, periodo: int) -> str:
        """Query para benchmark segmentado por porte empresarial.
        CNAE no ARGOS usa 5 dígitos (classe)."""
        cnae_5dig = cnae_classe[:5] if len(cnae_classe) >= 5 else cnae_classe
        return f"""
        SELECT 
            porte_empresa,
            qtd_empresas,
            qtd_empresas_ativas,
            faturamento_total,
            faturamento_medio,
            icms_devido_total,
            aliq_efetiva_media,
            aliq_efetiva_mediana,
            aliq_efetiva_desvio
        FROM niat.argos_benchmark_setorial_porte
        WHERE cnae_classe = '{cnae_5dig}'
          AND nu_per_ref = {periodo}
        ORDER BY 
            CASE porte_empresa 
                WHEN 'MEI' THEN 1 
                WHEN 'MICRO' THEN 2 
                WHEN 'PEQUENO' THEN 3 
                WHEN 'MEDIO' THEN 4 
                WHEN 'GRANDE' THEN 5 
                ELSE 6 
            END
        """


# =============================================================================
# FUNÇÕES DE ANÁLISE
# =============================================================================

def calcular_indice_concentracao(valores: List[float]) -> Dict[str, float]:
    """Calcula índices de concentração (Herfindahl e CR3/CR5)."""
    if not valores or sum(valores) == 0:
        return {'hhi': 0, 'cr3': 0, 'cr5': 0}
    
    total = sum(valores)
    participacoes = [v / total for v in valores]
    participacoes_sorted = sorted(participacoes, reverse=True)
    
    # Índice Herfindahl-Hirschman (HHI)
    hhi = sum([p ** 2 for p in participacoes]) * 10000
    
    # Concentração dos 3 e 5 maiores
    cr3 = sum(participacoes_sorted[:3]) * 100 if len(participacoes_sorted) >= 3 else sum(participacoes_sorted) * 100
    cr5 = sum(participacoes_sorted[:5]) * 100 if len(participacoes_sorted) >= 5 else sum(participacoes_sorted) * 100
    
    return {'hhi': hhi, 'cr3': cr3, 'cr5': cr5}

def identificar_outliers(df: pd.DataFrame, coluna: str, metodo: str = 'iqr') -> pd.DataFrame:
    """Identifica outliers usando IQR ou Z-score."""
    if df.empty or coluna not in df.columns:
        return df
    
    df_copy = df.copy()
    
    if metodo == 'iqr':
        Q1 = df_copy[coluna].quantile(0.25)
        Q3 = df_copy[coluna].quantile(0.75)
        IQR = Q3 - Q1
        limite_inferior = Q1 - 1.5 * IQR
        limite_superior = Q3 + 1.5 * IQR
        df_copy['outlier'] = (df_copy[coluna] < limite_inferior) | (df_copy[coluna] > limite_superior)
    else:  # z-score
        media = df_copy[coluna].mean()
        desvio = df_copy[coluna].std()
        df_copy['z_score'] = (df_copy[coluna] - media) / desvio if desvio > 0 else 0
        df_copy['outlier'] = abs(df_copy['z_score']) > 3
    
    return df_copy

def calcular_variacao(atual: float, anterior: float) -> Tuple[float, str]:
    """Calcula variação percentual e tendência."""
    if anterior == 0:
        if atual > 0:
            return 100.0, "📈"
        return 0.0, "➡️"
    
    variacao = ((atual - anterior) / anterior) * 100
    
    if variacao > 10:
        tendencia = "📈"
    elif variacao < -10:
        tendencia = "📉"
    else:
        tendencia = "➡️"
    
    return variacao, tendencia


# =============================================================================
# FUNÇÕES DE RENDERIZAÇÃO
# =============================================================================

def obter_cor_situacao_cadastral(situacao: str) -> str:
    """Retorna a cor de fundo baseada na situação cadastral."""
    situacao_upper = situacao.upper() if situacao else ''
    if situacao_upper == 'ATIVA':
        return "#28a745"  # Verde
    elif situacao_upper in ('CANCELADA', 'BAIXA REQUERIDA'):
        return "#dc3545"  # Vermelho
    elif situacao_upper == 'BAIXA DEFERIDA':
        return "#ffc107"  # Amarelo
    else:
        return "#6c757d"  # Cinza (normal)

def render_header(cadastro: Dict):
    """Renderiza cabeçalho com dados da empresa."""
    situacao = cadastro.get('situacao_cadastral_desc', 'N/A')
    cor_situacao = obter_cor_situacao_cadastral(situacao)
    cnae_desc = cadastro.get('descricao_cnae', 'N/A') or 'N/A'
    cnae_desc_curto = cnae_desc[:40] + '...' if len(cnae_desc) > 40 else cnae_desc

    st.markdown(f"""
    <div style='background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                padding: 10px 15px; border-radius: 8px; margin-bottom: 10px; color: white;'>
        <h3 style='margin: 0 0 5px 0; font-size: 1.2em;'>📄 {cadastro.get('razao_social', 'N/A')}</h3>
        <p style='margin: 2px 0; opacity: 0.9; font-size: 0.9em;'>
            <strong>CNPJ:</strong> {formatar_cnpj(cadastro.get('cnpj', ''))} |
            <strong>IE:</strong> {formatar_ie(cadastro.get('inscricao_estadual', ''))} |
            <span style='background-color: {cor_situacao}; padding: 1px 6px; border-radius: 3px;'>{situacao}</span>
        </p>
        <p style='margin: 2px 0; opacity: 0.8; font-size: 0.85em;'>
            <strong>CNAE:</strong> {cadastro.get('cnae', 'N/A')} - {cnae_desc_curto} |
            <strong>Regime:</strong> {cadastro.get('regime_apuracao_desc', 'N/A')} |
            <strong>Município:</strong> {cadastro.get('municipio', 'N/A')}/{cadastro.get('uf', 'SC')}
        </p>
    </div>
    """, unsafe_allow_html=True)


def render_kpi_cards(metricas: Dict):
    """Renderiza cards de KPIs."""
    cols = st.columns(4)
    
    with cols[0]:
        st.metric(
            label="📤 NFe Emitidas",
            value=formatar_numero(metricas.get('nfe_emitidas_qtd', 0)),
            delta=formatar_moeda(metricas.get('nfe_emitidas_valor', 0))
        )
    
    with cols[1]:
        st.metric(
            label="📥 NFe Recebidas",
            value=formatar_numero(metricas.get('nfe_recebidas_qtd', 0)),
            delta=formatar_moeda(metricas.get('nfe_recebidas_valor', 0))
        )
    
    with cols[2]:
        st.metric(
            label="🛒 NFCe (Varejo)",
            value=formatar_numero(metricas.get('nfce_qtd', 0)),
            delta=formatar_moeda(metricas.get('nfce_valor', 0))
        )
    
    with cols[3]:
        ticket_medio = metricas.get('nfe_emitidas_valor', 0) / max(metricas.get('nfe_emitidas_qtd', 1), 1)
        st.metric(
            label="💰 Ticket Médio NFe",
            value=formatar_moeda(ticket_medio)
        )


def render_tab_cadastro(cadastro: Dict):
    """Renderiza aba de cadastro."""
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📋 Dados Cadastrais")
        
        dados = [
            ("Razão Social", cadastro.get('razao_social', '-')),
            ("Nome Fantasia", cadastro.get('nome_fantasia', '-') or '-'),
            ("CNPJ", formatar_cnpj(cadastro.get('cnpj', ''))),
            ("Inscrição Estadual", formatar_ie(cadastro.get('inscricao_estadual', ''))),
            ("Data Situação", str(cadastro.get('data_situacao_cadastral', '-'))),
            ("Natureza Jurídica", cadastro.get('natureza_juridica', '-')),
            ("Data Constituição", str(cadastro.get('data_constituicao', '-'))),
            ("Data Início ICMS", str(cadastro.get('data_inicio_icms', '-'))),
        ]

        for label, valor in dados:
            st.markdown(f"**{label}:** {valor}")

        # Situação cadastral com cor de fundo
        situacao = cadastro.get('situacao_cadastral_desc', '-')
        cor_situacao = obter_cor_situacao_cadastral(situacao)
        st.markdown(f"**Situação Cadastral:** <span style='background-color: {cor_situacao}; color: white; padding: 2px 8px; border-radius: 3px;'>{situacao}</span>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("### 📍 Localização e Contato")
        
        endereco = f"{cadastro.get('logradouro', '')} {cadastro.get('numero', '')}".strip()
        
        dados = [
            ("Endereço", endereco or '-'),
            ("Bairro", cadastro.get('bairro', '-')),
            ("Município/UF", f"{cadastro.get('municipio', '-')}/{cadastro.get('uf', 'SC')}"),
            ("CEP", cadastro.get('cep', '-')),
            ("Telefone", cadastro.get('telefone', '-') or '-'),
            ("E-mail", cadastro.get('email', '-') or '-'),
            ("USEFI", cadastro.get('usefi', '-') or '-'),
            ("GERFE", cadastro.get('gerfe', '-') or '-'),
        ]
        
        for label, valor in dados:
            st.markdown(f"**{label}:** {valor}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🏢 Classificação Fiscal")
        
        dados = [
            ("CNAE", f"{cadastro.get('cnae', '-')} - {cadastro.get('descricao_cnae', '-')}"),
            ("Seção CNAE", f"{cadastro.get('secao_cnae', '-')} - {cadastro.get('descricao_secao', '-')}"),
            ("Regime de Apuração", cadastro.get('regime_apuracao_desc', '-')),
            ("Tipo Contribuinte", cadastro.get('tipo_contribuinte_desc', '-')),
            ("Enquadramento", cadastro.get('enquadramento_empresa', '-') or '-'),
            ("Simples Nacional", "Sim" if cadastro.get('flag_simples_nacional') == 'S' else "Não"),
        ]
        
        for label, valor in dados:
            st.markdown(f"**{label}:** {valor}")
    
    with col2:
        st.markdown("### 📊 Contabilista")
        
        dados = [
            ("Nome", cadastro.get('nome_contador', '-') or '-'),
            ("CPF/CNPJ", cadastro.get('cpf_cnpj_contador', '-') or '-'),
            ("CRC", cadastro.get('crc_contador', '-') or '-'),
            ("Qtd. Sócios", cadastro.get('qtd_socios', '-')),
            ("Sócios Ativos", cadastro.get('qtd_socios_ativos', '-')),
        ]
        
        for label, valor in dados:
            st.markdown(f"**{label}:** {valor}")


def render_tab_visao_geral(dados: Dict, periodo_inicio: int, periodo_fim: int):
    """Renderiza aba de visão geral."""
    
    # KPIs
    metricas = dados.get('metricas', {})
    render_kpi_cards(metricas)
    
    st.markdown("---")
    
    # Gráfico de evolução temporal
    st.markdown("### 📈 Evolução Mensal")
    
    col1, col2 = st.columns(2)
    
    with col1:
        df_nfe_emit = dados.get('nfe_emitidas_resumo', pd.DataFrame())
        if not df_nfe_emit.empty:
            df_plot = df_nfe_emit.copy()
            df_plot['periodo_str'] = df_plot['periodo'].astype(str)
            df_plot = df_plot.sort_values('periodo')
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_plot['periodo_str'],
                y=df_plot['valor_total'],
                name='Valor NFe Emitidas',
                marker_color='#1e3c72'
            ))
            fig.update_layout(
                title='NFe Emitidas - Valor por Período',
                xaxis_title='Período',
                xaxis={'type': 'category'},
                yaxis_title='Valor (R$)',
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de NFe emitidas no período.")
    
    with col2:
        df_nfe_rec = dados.get('nfe_recebidas_resumo', pd.DataFrame())
        if not df_nfe_rec.empty:
            df_plot = df_nfe_rec.copy()
            df_plot['periodo_str'] = df_plot['periodo'].astype(str)
            df_plot = df_plot.sort_values('periodo')
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_plot['periodo_str'],
                y=df_plot['valor_total'],
                name='Valor NFe Recebidas',
                marker_color='#2a5298'
            ))
            fig.update_layout(
                title='NFe Recebidas - Valor por Período',
                xaxis_title='Período',
                xaxis={'type': 'category'},
                yaxis_title='Valor (R$)',
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sem dados de NFe recebidas no período.")
    
    # NFCe
    df_nfce = dados.get('nfce_resumo', pd.DataFrame())
    if not df_nfce.empty:
        df_plot = df_nfce.copy()
        df_plot['periodo_str'] = df_plot['periodo'].astype(str)
        df_plot = df_plot.sort_values('periodo')
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_plot['periodo_str'],
            y=df_plot['valor_total'],
            name='Valor NFCe',
            marker_color='#28a745'
        ))
        fig.add_trace(go.Scatter(
            x=df_plot['periodo_str'],
            y=df_plot['qtd_notas'],
            name='Qtd. Notas',
            yaxis='y2',
            line=dict(color='#dc3545', width=2)
        ))
        fig.update_layout(
            title='NFCe - Valor e Quantidade por Período',
            xaxis_title='Período',
            xaxis={'type': 'category'},
            yaxis=dict(title='Valor (R$)', side='left'),
            yaxis2=dict(title='Quantidade', side='right', overlaying='y'),
            height=350,
            legend=dict(x=0, y=1.15, orientation='h')
        )
        st.plotly_chart(fig, use_container_width=True)


def render_tab_produtos(dados: Dict, tipo: str = 'nfe'):
    """Renderiza aba de produtos - apenas tabelas sem gráficos."""
    
    df_ncm = dados.get(f'top_ncm_{tipo}', pd.DataFrame())
    df_prod = dados.get(f'top_produtos_{tipo}', pd.DataFrame())
    
    # Verificar se há dados disponíveis
    if df_ncm.empty and df_prod.empty:
        st.warning(f"""
        ⚠️ **Dados de produtos não encontrados**
        
        Não foram encontrados dados de produtos {'NFe' if tipo == 'nfe' else 'NFCe'} para o período selecionado.
        Isso pode ocorrer se não houver notas emitidas no período ou se houver erro na consulta.
        """)
        return
    
    # Tabela NCM
    st.markdown("### 📦 Top NCM por Valor")
    
    if not df_ncm.empty:
        df_display = df_ncm.copy()
        if 'descricao_ncm' in df_display.columns:
            df_display['descricao_ncm'] = df_display['descricao_ncm'].apply(
                lambda x: x[:60] + '...' if len(str(x)) > 60 else x
            )
        
        # Calcular max para barras de progresso
        max_valor = df_display['valor_total'].max() if not df_display['valor_total'].empty else 1
        max_itens = df_display['qtd_itens'].max() if not df_display['qtd_itens'].empty else 1
        
        # Selecionar colunas para exibição
        colunas = ['ncm', 'valor_total', 'qtd_notas', 'qtd_itens']
        if 'descricao_ncm' in df_display.columns:
            colunas.insert(1, 'descricao_ncm')
        
        st.dataframe(
            df_display[colunas], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                'ncm': 'NCM',
                'descricao_ncm': 'Descrição',
                'valor_total': col_barra_valor('Valor Total', max_valor),
                'qtd_notas': col_numero('Notas'),
                'qtd_itens': col_barra_qtd('Itens', max_itens)
            }
        )
    else:
        st.info("Sem dados de NCM no período.")
    
    st.markdown("---")
    
    # Tabela Produtos
    st.markdown("### 🏷️ Top Produtos por Valor")
    
    if not df_prod.empty:
        df_display = df_prod.head(20).copy()
        df_display['descricao'] = df_display['descricao'].apply(lambda x: x[:50] + '...' if len(str(x)) > 50 else x)
        
        # Calcular max para barras
        max_valor = df_display['valor_total'].max() if not df_display['valor_total'].empty else 1
        
        # Selecionar colunas existentes
        colunas_disp = ['descricao', 'ncm', 'valor_total', 'qtd_notas', 'qtd_itens']
        config = {
            'codigo': 'Código',
            'descricao': 'Descrição',
            'ncm': 'NCM',
            'valor_total': col_barra_valor('Valor Total', max_valor),
            'qtd_notas': col_numero('Notas'),
            'qtd_itens': col_numero('Itens'),
            'qtd_vendida': col_numero('Qtd. Vendida')
        }
        
        if 'codigo' in df_display.columns:
            colunas_disp.insert(0, 'codigo')
        if 'qtd_vendida' in df_display.columns:
            colunas_disp.append('qtd_vendida')
        
        st.dataframe(
            df_display[colunas_disp], 
            use_container_width=True, 
            hide_index=True,
            column_config=config
        )
    else:
        st.info("Sem dados de produtos no período.")


def render_tab_clientes(dados: Dict):
    """Renderiza aba de clientes - tabela + concentração."""
    
    df_clientes = dados.get('top_clientes', pd.DataFrame())
    
    if df_clientes.empty:
        st.info("Sem dados de clientes no período selecionado.")
        return
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Tabela detalhada
        st.markdown("### 👥 Top 10 Clientes por Valor")
        
        df_display = df_clientes.copy()
        total = df_display['valor_total'].sum()
        df_display['participacao'] = df_display['valor_total'] / total * 100
        df_display['cnpj_fmt'] = df_display['cnpj_cliente'].apply(formatar_cnpj)
        
        # Max para barras
        max_valor = df_display['valor_total'].max()
        
        st.dataframe(
            df_display[['cnpj_fmt', 'razao_social', 'uf_cliente', 'qtd_notas', 'valor_total', 'participacao']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'cnpj_fmt': 'CNPJ',
                'razao_social': 'Razão Social',
                'uf_cliente': 'UF',
                'qtd_notas': col_numero('Notas'),
                'valor_total': col_barra_valor('Valor Total', max_valor),
                'participacao': col_barra_pct('% Part.')
            }
        )
    
    with col2:
        st.markdown("### 📊 Concentração")
        
        valores = df_clientes['valor_total'].tolist()
        indices = calcular_indice_concentracao(valores)
        
        st.metric("Índice HHI", f"{indices['hhi']:,.0f}")
        st.caption("< 1.500 = Baixa | 1.500-2.500 = Moderada | > 2.500 = Alta")
        
        st.metric("CR3 (Top 3)", formatar_percentual(indices['cr3']))
        st.metric("CR5 (Top 5)", formatar_percentual(indices['cr5']))
        
        # Alertas
        if indices['hhi'] > 2500:
            st.warning("⚠️ **Alta concentração** de vendas em poucos clientes.")
        elif indices['cr3'] > 50:
            st.warning("⚠️ Os 3 maiores clientes representam mais de 50% das vendas.")


def render_tab_fornecedores(dados: Dict):
    """Renderiza aba de fornecedores - tabela + concentração."""
    
    df_fornecedores = dados.get('top_fornecedores', pd.DataFrame())
    
    if df_fornecedores.empty:
        st.info("Sem dados de fornecedores no período selecionado.")
        return
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Tabela detalhada
        st.markdown("### 🏭 Top 10 Fornecedores por Valor")
        
        df_display = df_fornecedores.copy()
        total = df_display['valor_total'].sum()
        df_display['participacao'] = df_display['valor_total'] / total * 100
        df_display['cnpj_fmt'] = df_display['cnpj_fornecedor'].apply(formatar_cnpj)
        
        # Max para barras
        max_valor = df_display['valor_total'].max()
        
        st.dataframe(
            df_display[['cnpj_fmt', 'razao_social', 'uf_fornecedor', 'qtd_notas', 'valor_total', 'participacao']],
            use_container_width=True,
            hide_index=True,
            column_config={
                'cnpj_fmt': 'CNPJ',
                'razao_social': 'Razão Social',
                'uf_fornecedor': 'UF',
                'qtd_notas': col_numero('Notas'),
                'valor_total': col_barra_valor('Valor Total', max_valor),
                'participacao': col_barra_pct('% Part.')
            }
        )
    
    with col2:
        st.markdown("### 📊 Concentração")
        
        valores = df_fornecedores['valor_total'].tolist()
        indices = calcular_indice_concentracao(valores)
        
        st.metric("Índice HHI", f"{indices['hhi']:,.0f}")
        st.caption("< 1.500 = Baixa | 1.500-2.500 = Moderada | > 2.500 = Alta")
        
        st.metric("CR3 (Top 3)", formatar_percentual(indices['cr3']))
        st.metric("CR5 (Top 5)", formatar_percentual(indices['cr5']))
        
        # Alertas
        if indices['hhi'] > 2500:
            st.warning("⚠️ **Alta dependência** de poucos fornecedores.")
        elif indices['cr3'] > 60:
            st.warning("⚠️ Os 3 maiores fornecedores representam mais de 60% das compras.")


def render_tab_faturamento(dados: Dict, cadastro: Dict):
    """Renderiza aba de faturamento."""
    
    is_simples = cadastro.get('flag_simples_nacional') == 'S'
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 💰 Faturamento Declarado")
        
        if is_simples:
            df_fat = dados.get('faturamento_pgdas', pd.DataFrame())
            if not df_fat.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df_fat['periodo'].astype(str),
                    y=df_fat['receita_bruta'],
                    name='Receita Bruta (PGDAS)',
                    marker_color='#1e3c72'
                ))
                fig.update_layout(
                    title='Faturamento PGDAS (Simples Nacional)',
                    xaxis_title='Período',
                    yaxis_title='Valor (R$)',
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Métricas
                total_fat = df_fat['receita_bruta'].sum()
                media_fat = df_fat['receita_bruta'].mean()
                st.metric("Total no Período", formatar_moeda(total_fat))
                st.metric("Média Mensal", formatar_moeda(media_fat))
            else:
                st.info("Sem dados de PGDAS no período.")
        else:
            df_fat = dados.get('faturamento_dime', pd.DataFrame())
            if not df_fat.empty:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=df_fat['periodo'].astype(str),
                    y=df_fat['faturamento'],
                    name='Faturamento (DIME)',
                    marker_color='#1e3c72'
                ))
                fig.update_layout(
                    title='Faturamento DIME (Regime Normal)',
                    xaxis_title='Período',
                    yaxis_title='Valor (R$)',
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Métricas
                total_fat = df_fat['faturamento'].sum()
                media_fat = df_fat['faturamento'].mean()
                st.metric("Total no Período", formatar_moeda(total_fat))
                st.metric("Média Mensal", formatar_moeda(media_fat))
            else:
                st.info("Sem dados de DIME no período.")
    
    with col2:
        st.markdown("### 📊 Comparativo: Declarado vs NFe/NFCe")
        
        df_nfe = dados.get('nfe_emitidas_resumo', pd.DataFrame())
        df_nfce = dados.get('nfce_resumo', pd.DataFrame())
        
        if is_simples:
            df_fat = dados.get('faturamento_pgdas', pd.DataFrame())
            col_valor = 'receita_bruta'
        else:
            df_fat = dados.get('faturamento_dime', pd.DataFrame())
            col_valor = 'faturamento'
        
        if not df_fat.empty and (not df_nfe.empty or not df_nfce.empty):
            # Preparar dados
            df_fat['periodo'] = df_fat['periodo'].astype(str)
            
            # Iniciar com faturamento declarado
            df_comp = df_fat[['periodo', col_valor]].copy()
            df_comp = df_comp.rename(columns={col_valor: 'declarado'})
            
            # Adicionar NFe se houver
            if not df_nfe.empty:
                df_nfe_prep = df_nfe[['periodo', 'valor_total']].copy()
                df_nfe_prep['periodo'] = df_nfe_prep['periodo'].astype(str)
                df_nfe_prep = df_nfe_prep.rename(columns={'valor_total': 'nfe_valor'})
                df_comp = pd.merge(df_comp, df_nfe_prep, on='periodo', how='outer')
            else:
                df_comp['nfe_valor'] = 0
            
            # Adicionar NFCe se houver
            if not df_nfce.empty:
                df_nfce_prep = df_nfce[['periodo', 'valor_total']].copy()
                df_nfce_prep['periodo'] = df_nfce_prep['periodo'].astype(str)
                df_nfce_prep = df_nfce_prep.rename(columns={'valor_total': 'nfce_valor'})
                df_comp = pd.merge(df_comp, df_nfce_prep, on='periodo', how='outer')
            else:
                df_comp['nfce_valor'] = 0
            
            df_comp = df_comp.fillna(0)
            df_comp['total_notas'] = df_comp['nfe_valor'] + df_comp['nfce_valor']
            df_comp['diferenca'] = df_comp['total_notas'] - df_comp['declarado']
            df_comp['diferenca_pct'] = np.where(
                df_comp['declarado'] > 0,
                (df_comp['diferenca'] / df_comp['declarado']) * 100,
                0
            )
            df_comp = df_comp.sort_values('periodo')
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_comp['periodo'],
                y=df_comp['declarado'],
                name='Declarado',
                marker_color='#1e3c72'
            ))
            fig.add_trace(go.Bar(
                x=df_comp['periodo'],
                y=df_comp['nfe_valor'],
                name='NFe Emitidas',
                marker_color='#28a745'
            ))
            
            # Adicionar NFCe ao gráfico se houver valores
            if df_comp['nfce_valor'].sum() > 0:
                fig.add_trace(go.Bar(
                    x=df_comp['periodo'],
                    y=df_comp['nfce_valor'],
                    name='NFCe',
                    marker_color='#ffc107'
                ))
            
            fig.update_layout(
                title='Comparativo Mensal',
                xaxis_title='Período',
                xaxis={'type': 'category'},
                yaxis_title='Valor (R$)',
                barmode='group',
                height=350
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Alertas de divergência
            divergencias = df_comp[abs(df_comp['diferenca_pct']) > 20]
            if not divergencias.empty:
                st.warning(f"⚠️ Há {len(divergencias)} período(s) com divergência superior a 20% entre o declarado e as notas emitidas (NFe + NFCe).")
        else:
            st.info("Dados insuficientes para comparativo.")


def render_tab_tributacao(dados: Dict):
    """Renderiza aba de tributação."""
    
    df_trib = dados.get('tributacao_nfe', pd.DataFrame())
    
    if df_trib.empty:
        st.warning("""
        ⚠️ **Dados de tributação não encontrados**
        
        Não foram encontrados dados de tributação para o período selecionado.
        Isso pode ocorrer se não houver NFe emitidas no período ou se houver erro na consulta.
        """)
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📊 Distribuição por CST/CSOSN")
        
        df_cst = df_trib.groupby('cst').agg({
            'valor_produtos': 'sum',
            'icms_total': 'sum',
            'qtd_itens': 'sum'
        }).reset_index()
        
        fig = px.pie(
            df_cst,
            values='valor_produtos',
            names='cst',
            title='Valor por CST/CSOSN',
            hole=0.4
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### 🌍 Distribuição por Origem")
        
        origem_map = {0: 'Nacional', 1: 'Estrangeira (Importação Direta)', 
                      2: 'Estrangeira (Mercado Interno)', 3: 'Nacional (40-70% Conteúdo)',
                      4: 'Nacional (Processos Básicos)', 5: 'Nacional (<40% Conteúdo)',
                      6: 'Estrangeira (Sem Similar)', 7: 'Estrangeira (Sem Similar)',
                      8: 'Nacional (70% Conteúdo Importado)'}
        
        df_origem = df_trib.groupby('origem').agg({
            'valor_produtos': 'sum',
            'icms_total': 'sum'
        }).reset_index()
        df_origem['origem_desc'] = df_origem['origem'].map(origem_map).fillna('Outros')
        
        fig = px.pie(
            df_origem,
            values='valor_produtos',
            names='origem_desc',
            title='Valor por Origem do Produto',
            hole=0.4
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Resumo por Grupo de Tributação
    if 'grupo_tributacao' in df_trib.columns:
        st.markdown("### 📋 Resumo por Grupo de Tributação")
        
        df_grupo = df_trib.groupby('grupo_tributacao').agg({
            'valor_produtos': 'sum',
            'icms_total': 'sum',
            'base_calculo_total': 'sum',
            'qtd_itens': 'sum',
            'qtd_notas': 'sum'
        }).reset_index()
        
        fig = px.bar(
            df_grupo,
            x='grupo_tributacao',
            y='valor_produtos',
            color='icms_total',
            title='Valor de Produtos e ICMS por Grupo de Tributação',
            labels={'valor_produtos': 'Valor Produtos (R$)', 'grupo_tributacao': 'Grupo'}
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("### 💹 Detalhamento por CST")
    
    # Tabela detalhada
    df_display = df_trib.copy()
    df_display['aliquota_efetiva'] = np.where(
        df_display['valor_produtos'] > 0,
        (df_display['icms_total'] / df_display['valor_produtos']) * 100,
        0
    )
    
    # Ordenar
    df_display = df_display.sort_values('valor_produtos', ascending=False).head(20)
    
    # Max para barras
    max_valor = df_display['valor_produtos'].max() if not df_display['valor_produtos'].empty else 1
    max_icms = df_display['icms_total'].max() if not df_display['icms_total'].empty else 1
    
    # Selecionar colunas para exibição
    colunas_exib = ['cst', 'origem', 'valor_produtos', 'base_calculo_total', 'icms_total', 'aliquota_media', 'aliquota_efetiva', 'qtd_itens', 'qtd_notas']
    if 'grupo_tributacao' in df_display.columns:
        colunas_exib.insert(2, 'grupo_tributacao')
    
    st.dataframe(
        df_display[colunas_exib],
        use_container_width=True,
        hide_index=True,
        column_config={
            'cst': 'CST/CSOSN',
            'origem': col_numero('Origem'),
            'grupo_tributacao': 'Grupo Trib.',
            'valor_produtos': col_barra_valor('Valor Produtos', max_valor),
            'base_calculo_total': col_moeda('Base Cálculo'),
            'icms_total': col_barra_valor('ICMS', max_icms),
            'aliquota_media': col_percentual('Alíq. Média'),
            'aliquota_efetiva': col_percentual('Alíq. Efetiva'),
            'qtd_itens': col_numero('Itens'),
            'qtd_notas': col_numero('Notas')
        }
    )


def render_tab_setor(dados: Dict, cadastro: Dict):
    """Renderiza aba de comparação setorial com análises ARGOS."""
    
    cnae = cadastro.get('cnae', '')
    cnpj = limpar_cnpj(cadastro.get('cnpj', ''))
    
    st.markdown(f"### 🎯 Análise Setorial: {cnae} - {cadastro.get('descricao_cnae', '')}")
    
    if not cnae:
        st.warning("CNAE não disponível para análise setorial.")
        return
    
    # Buscar período mais recente disponível no ARGOS
    try:
        df_periodo = executar_query_cached(
            NotasQueries.get_periodo_mais_recente_argos(),
            _cache_key="argos_periodo_recente"
        )
        if not df_periodo.empty and df_periodo.iloc[0]['periodo_mais_recente']:
            periodo_argos = int(df_periodo.iloc[0]['periodo_mais_recente'])
        else:
            periodo_argos = 202508  # fallback
    except:
        periodo_argos = 202508  # fallback
    
    st.caption(f"📅 Dados ARGOS referentes ao período: **{periodo_argos}**")
    
    # =========================================================================
    # SEÇÃO 1: Dados básicos do setor (query original)
    # =========================================================================
    df_setor = dados.get('setor_stats', pd.DataFrame())
    
    if not df_setor.empty:
        stats = df_setor.iloc[0].to_dict()
        metricas = dados.get('metricas', {})
        faturamento_empresa = metricas.get('nfe_emitidas_valor', 0)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🏢 Empresas no Setor", formatar_numero(stats.get('qtd_empresas', 0)))
        with col2:
            st.metric("📊 Média do Setor", formatar_moeda(stats.get('media_faturamento', 0)))
        with col3:
            st.metric("📈 Mediana do Setor", formatar_moeda(stats.get('mediana_faturamento', 0)))
        with col4:
            st.metric("💰 Seu Faturamento NFe", formatar_moeda(faturamento_empresa))
    
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 2: Benchmark ARGOS (se disponível)
    # =========================================================================
    st.markdown("### 📊 Benchmark Setorial (ARGOS)")
    
    # Tentar buscar dados ARGOS usando período mais recente e CNAE 5 dígitos
    try:
        df_benchmark = executar_query_cached(
            NotasQueries.get_benchmark_setorial(cnae, periodo_argos),
            _cache_key=f"argos_bench_{cnae[:5]}_{periodo_argos}"
        )
        
        df_empresa_bench = executar_query_cached(
            NotasQueries.get_empresa_vs_benchmark(cnpj, periodo_argos),
            _cache_key=f"argos_emp_bench_{cnpj}_{periodo_argos}"
        )
    except Exception as e:
        df_benchmark = pd.DataFrame()
        df_empresa_bench = pd.DataFrame()
    
    if not df_benchmark.empty:
        bench = df_benchmark.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("🏭 Total Empresas", f"{int(bench.get('qtd_empresas_total', 0)):,}")
        with col2:
            fat_total = bench.get('faturamento_total', 0) / 1e9
            st.metric("💰 Faturamento Setor", f"R$ {fat_total:.2f}B")
        with col3:
            aliq_med = bench.get('aliq_efetiva_mediana', 0) * 100
            st.metric("📊 Alíq. Efetiva Mediana", f"{aliq_med:.2f}%")
        with col4:
            cv = bench.get('aliq_coef_variacao', 0)
            st.metric("📈 Coef. Variação", f"{cv:.3f}")
        
        # Status da empresa vs setor
        if not df_empresa_bench.empty:
            emp = df_empresa_bench.iloc[0]
            
            st.markdown("---")
            st.markdown("### 🎯 Sua Empresa vs Setor")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Métricas comparativas - tratar valores NULL
                aliq_emp_raw = emp.get('aliq_efetiva_empresa')
                aliq_setor_raw = emp.get('aliq_setor_mediana')
                
                aliq_empresa = (aliq_emp_raw * 100) if pd.notna(aliq_emp_raw) else 0
                aliq_setor = (aliq_setor_raw * 100) if pd.notna(aliq_setor_raw) else 0
                status = emp.get('status_vs_setor', 'N/A')
                if pd.isna(status):
                    status = 'SEM_DADOS'
                
                status_colors = {
                    'MUITO_ABAIXO': ('🔴', '#dc3545'),
                    'ABAIXO': ('🟠', '#fd7e14'),
                    'NORMAL': ('🟢', '#28a745'),
                    'ACIMA': ('🟡', '#ffc107'),
                    'MUITO_ACIMA': ('🔴', '#dc3545'),
                    'SEM_DADOS': ('⚪', '#6c757d')
                }
                
                emoji, cor = status_colors.get(status, ('⚪', '#6c757d'))
                
                st.markdown(f"""
                <div style='background-color: {cor}; color: white; padding: 20px; border-radius: 10px; text-align: center;'>
                    <h2 style='margin: 0;'>{emoji} {status.replace('_', ' ')}</h2>
                    <p style='margin: 5px 0;'>Alíquota Empresa: <b>{aliq_empresa:.2f}%</b></p>
                    <p style='margin: 5px 0;'>Alíquota Setor: <b>{aliq_setor:.2f}%</b></p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                # Gráfico comparativo de alíquotas - tratar valores NULL
                aliq_p25_raw = emp.get('aliq_setor_p25')
                aliq_p75_raw = emp.get('aliq_setor_p75')
                
                aliq_p25 = (aliq_p25_raw * 100) if pd.notna(aliq_p25_raw) else 0
                aliq_p75 = (aliq_p75_raw * 100) if pd.notna(aliq_p75_raw) else 0
                
                dados_comp = pd.DataFrame({
                    'Tipo': ['Sua Empresa', 'Setor P25', 'Setor Mediana', 'Setor P75'],
                    'Alíquota': [aliq_empresa, aliq_p25, aliq_setor, aliq_p75]
                })
                
                fig = px.bar(
                    dados_comp,
                    x='Tipo',
                    y='Alíquota',
                    title="Comparação de Alíquota Efetiva (%)",
                    color='Tipo',
                    color_discrete_sequence=['#dc3545', '#28a745', '#1e3c72', '#ffc107']
                )
                fig.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("ℹ️ Dados ARGOS não disponíveis para este setor/período. Exibindo análise baseada nas notas fiscais.")
    
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 3: Benchmark por Porte
    # =========================================================================
    st.markdown("### 📊 Distribuição por Porte Empresarial")
    
    try:
        df_porte = executar_query_cached(
            NotasQueries.get_benchmark_por_porte(cnae, periodo_argos),
            _cache_key=f"argos_porte_{cnae[:5]}_{periodo_argos}"
        )
    except:
        df_porte = pd.DataFrame()
    
    if not df_porte.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            df_porte['aliq_mediana_pct'] = df_porte['aliq_efetiva_mediana'] * 100
            
            fig = px.bar(
                df_porte,
                x='porte_empresa',
                y='aliq_mediana_pct',
                title="Alíquota Mediana por Porte",
                labels={'porte_empresa': 'Porte', 'aliq_mediana_pct': 'Alíquota (%)'},
                color='porte_empresa',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            df_porte['faturamento_medio_mi'] = df_porte['faturamento_medio'] / 1e6
            
            fig = px.bar(
                df_porte,
                x='porte_empresa',
                y='qtd_empresas',
                title="Quantidade de Empresas por Porte",
                labels={'porte_empresa': 'Porte', 'qtd_empresas': 'Qtd Empresas'},
                color='porte_empresa',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Tabela detalhada
        st.dataframe(
            df_porte[['porte_empresa', 'qtd_empresas', 'aliq_mediana_pct']],
            hide_index=True,
            use_container_width=True,
            column_config={
                'porte_empresa': 'Porte',
                'qtd_empresas': col_numero('Empresas'),
                'aliq_mediana_pct': col_percentual('Alíq. Mediana')
            }
        )
    
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 4: Alertas da Empresa
    # =========================================================================
    st.markdown("### ⚠️ Alertas e Riscos")
    
    try:
        df_alertas = executar_query_cached(
            NotasQueries.get_alertas_empresa(cnpj, periodo_argos),
            _cache_key=f"argos_alertas_{cnpj}_{periodo_argos}"
        )
    except:
        df_alertas = pd.DataFrame()
    
    if not df_alertas.empty:
        for _, alerta in df_alertas.iterrows():
            severidade = alerta.get('severidade', 'BAIXA')
            tipo = alerta.get('tipo_alerta', 'Alerta')
            score = alerta.get('score_risco', 0)
            if pd.isna(score):
                score = 0
            
            if severidade == 'ALTA':
                st.error(f"🔴 **{tipo}** - Score: {score:.1f}")
            elif severidade == 'MEDIA':
                st.warning(f"🟠 **{tipo}** - Score: {score:.1f}")
            else:
                st.info(f"🟢 **{tipo}** - Score: {score:.1f}")
    else:
        st.success("✅ Nenhum alerta registrado para esta empresa.")
    
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 5: Empresas do Setor (Top 20)
    # =========================================================================
    st.markdown("### 🏢 Empresas do Setor (Top 20 por Faturamento)")
    
    try:
        df_empresas_setor = executar_query_cached(
            NotasQueries.get_empresas_setor(cnae, periodo_argos, 20),
            _cache_key=f"argos_empresas_{cnae[:5]}_{periodo_argos}"
        )
    except:
        df_empresas_setor = pd.DataFrame()
    
    if not df_empresas_setor.empty:
        # Destacar a empresa atual na lista
        df_display = df_empresas_setor.copy()
        df_display['aliq_pct'] = df_display['aliq_efetiva_empresa'] * 100
        df_display['faturamento_mi'] = df_display['vl_faturamento'] / 1e6
        
        # Formatar CNPJ
        df_display['cnpj_fmt'] = df_display['nu_cnpj'].apply(
            lambda x: formatar_cnpj(str(x).zfill(14)) if pd.notna(x) else ''
        )
        
        # Verificar se a empresa atual está na lista
        empresa_na_lista = df_display[df_display['nu_cnpj'].astype(str).str.replace('[^0-9]', '', regex=True) == cnpj]
        if not empresa_na_lista.empty:
            posicao = empresa_na_lista.index[0] + 1
            st.success(f"🎯 Sua empresa está na posição **{posicao}º** entre as maiores do setor.")
        
        max_fat = df_display['vl_faturamento'].max()
        
        st.dataframe(
            df_display[['cnpj_fmt', 'nm_razao_social', 'porte_empresa', 'vl_faturamento', 'aliq_pct', 'status_vs_setor']],
            hide_index=True,
            use_container_width=True,
            column_config={
                'cnpj_fmt': 'CNPJ',
                'nm_razao_social': 'Razão Social',
                'porte_empresa': 'Porte',
                'vl_faturamento': col_barra_valor('Faturamento', max_fat),
                'aliq_pct': col_percentual('Alíq. Efetiva'),
                'status_vs_setor': 'Status'
            }
        )
    else:
        st.info("Lista de empresas do setor não disponível.")
    
    st.markdown("---")
    
    # =========================================================================
    # SEÇÃO 6: Alertas do Setor (Empresas com maior risco)
    # =========================================================================
    st.markdown("### 🎯 Empresas do Setor com Alertas de Risco")
    
    try:
        df_alertas_setor = executar_query_cached(
            NotasQueries.get_alertas_setor(cnae, periodo_argos, 15),
            _cache_key=f"argos_alertas_setor_{cnae[:5]}_{periodo_argos}"
        )
    except:
        df_alertas_setor = pd.DataFrame()
    
    if not df_alertas_setor.empty:
        df_alertas_setor['cnpj_fmt'] = df_alertas_setor['nu_cnpj'].apply(
            lambda x: formatar_cnpj(str(x).zfill(14)) if pd.notna(x) else ''
        )
        
        max_score = df_alertas_setor['score_risco'].max()
        if pd.isna(max_score):
            max_score = 100
        
        st.dataframe(
            df_alertas_setor[['cnpj_fmt', 'nm_razao_social', 'porte_empresa', 'tipo_alerta', 'severidade', 'score_risco']],
            hide_index=True,
            use_container_width=True,
            column_config={
                'cnpj_fmt': 'CNPJ',
                'nm_razao_social': 'Razão Social',
                'porte_empresa': 'Porte',
                'tipo_alerta': 'Tipo Alerta',
                'severidade': 'Severidade',
                'score_risco': st.column_config.ProgressColumn(
                    'Score Risco',
                    format="%.1f",
                    min_value=0,
                    max_value=float(max_score) if max_score else 100
                )
            }
        )
    else:
        st.info("Sem alertas de risco registrados para empresas do setor.")
    
    # =========================================================================
    # SEÇÃO 7: Posicionamento Visual (dados originais)
    # =========================================================================
    if not df_setor.empty:
        st.markdown("---")
        st.markdown("### 🏆 Seu Posicionamento por Faturamento")
        
        stats = df_setor.iloc[0].to_dict()
        metricas = dados.get('metricas', {})
        faturamento_empresa = metricas.get('nfe_emitidas_valor', 0)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de distribuição
            percentis = {
                'Mínimo': stats.get('min_faturamento', 0),
                'Mediana': stats.get('mediana_faturamento', 0),
                'Média': stats.get('media_faturamento', 0),
                'Máximo': stats.get('max_faturamento', 0)
            }
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=list(percentis.keys()),
                y=list(percentis.values()),
                marker_color='#1e3c72'
            ))
            
            fig.add_hline(
                y=faturamento_empresa,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Sua empresa: {formatar_moeda(faturamento_empresa)}"
            )
            
            fig.update_layout(
                title='Estatísticas de Faturamento do Setor',
                yaxis_title='Valor (R$)',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            mediana = stats.get('mediana_faturamento') or 0
            media = stats.get('media_faturamento') or 0
            minimo = stats.get('min_faturamento') or 0
            maximo = stats.get('max_faturamento') or 0

            if maximo > minimo:
                percentil_aprox = ((faturamento_empresa - minimo) / (maximo - minimo)) * 100
            else:
                percentil_aprox = 50
            
            if percentil_aprox >= 90:
                posicao, cor, emoji = "Top 10%", "#28a745", "🏆"
            elif percentil_aprox >= 75:
                posicao, cor, emoji = "Top 25%", "#17a2b8", "🥈"
            elif faturamento_empresa >= mediana:
                posicao, cor, emoji = "Acima da Mediana", "#ffc107", "📊"
            elif faturamento_empresa >= minimo + (mediana - minimo) / 2:
                posicao, cor, emoji = "Abaixo da Mediana", "#fd7e14", "📉"
            else:
                posicao, cor, emoji = "Quartil Inferior", "#dc3545", "⚠️"
            
            st.markdown(f"""
            <div style='background-color: {cor}; color: white; padding: 30px; border-radius: 10px; text-align: center;'>
                <h1 style='margin: 0;'>{emoji}</h1>
                <h2 style='margin: 10px 0;'>{posicao}</h2>
                <p>Baseado no faturamento NFe no período</p>
            </div>
            """, unsafe_allow_html=True)
            
            if media > 0:
                desvio = ((faturamento_empresa - media) / media) * 100
                if desvio > 0:
                    st.success(f"✅ Seu faturamento está **{desvio:.1f}% acima** da média do setor.")
                else:
                    st.warning(f"⚠️ Seu faturamento está **{abs(desvio):.1f}% abaixo** da média do setor.")


def render_tab_ttd(dados: Dict, cadastro: Dict):
    """Renderiza aba de TTDs (benefícios fiscais) com seções colapsáveis."""
    
    df_ttd = dados.get('ttd_empresa', pd.DataFrame())
    
    st.markdown("### 🎫 Tratamentos Tributários Diferenciados (TTDs)")
    
    if df_ttd.empty:
        st.info("ℹ️ Esta empresa não possui TTDs ativos registrados.")
        return
    
    # Contagem de TTDs
    total_ttds = len(df_ttd)
    st.success(f"✅ **{total_ttds} TTD(s) ativo(s)** encontrado(s) para esta empresa.")
    
    st.markdown("---")
    
    # Dicionário de categorias de TTD
    ttd_categorias = {
        'importacao': {
            'nome': '🚢 Importação',
            'codigos': [409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420],
            'descricao': 'Benefícios para operações de importação'
        },
        'atacadista': {
            'nome': '🏪 Atacadista',
            'codigos': [9],
            'descricao': 'Crédito presumido para atacadistas'
        },
        'diferimento': {
            'nome': '📋 Diferimento',
            'codigos': [1010, 1011, 1012],
            'descricao': 'Diferimento do ICMS'
        },
        'outros': {
            'nome': '📄 Outros Benefícios',
            'codigos': [],  # Todos que não se encaixam nas categorias acima
            'descricao': 'Outros tratamentos tributários diferenciados'
        }
    }
    
    # Agrupar TTDs por categoria
    ttds_por_categoria = {}
    ttds_outros = []
    
    for _, row in df_ttd.iterrows():
        cd_beneficio = row['cd_beneficio']
        encontrou_categoria = False
        
        for cat_key, cat_info in ttd_categorias.items():
            if cat_key != 'outros' and cd_beneficio in cat_info['codigos']:
                if cat_key not in ttds_por_categoria:
                    ttds_por_categoria[cat_key] = []
                ttds_por_categoria[cat_key].append(row)
                encontrou_categoria = True
                break
        
        if not encontrou_categoria:
            ttds_outros.append(row)
    
    if ttds_outros:
        ttds_por_categoria['outros'] = ttds_outros
    
    # Renderizar cada categoria como expander
    for cat_key, ttds in ttds_por_categoria.items():
        cat_info = ttd_categorias[cat_key]
        qtd = len(ttds)
        
        with st.expander(f"{cat_info['nome']} ({qtd} TTD{'s' if qtd > 1 else ''})", expanded=(qtd <= 2)):
            st.caption(cat_info['descricao'])
            
            for ttd in ttds:
                cd = ttd['cd_beneficio']
                descricao = ttd['de_beneficio']
                periodo_inicio = ttd['periodo_inicio']
                periodo_fim = ttd['periodo_fim']
                estado = ttd['estado']
                
                # Determinar cor do estado
                if estado == 'ATIVO':
                    cor_estado = '#28a745'
                    icone_estado = '✅'
                else:
                    cor_estado = '#ffc107'
                    icone_estado = '⚠️'
                
                # Card do TTD
                st.markdown(f"""
                <div style='background: linear-gradient(135deg, #f8f9fa, #e9ecef); 
                            padding: 15px; border-radius: 8px; margin-bottom: 10px;
                            border-left: 4px solid {cor_estado};'>
                    <div style='display: flex; justify-content: space-between; align-items: center;'>
                        <div>
                            <h4 style='margin: 0; color: #1e3c72;'>TTD {cd}</h4>
                            <p style='margin: 5px 0; color: #666; font-size: 0.9em;'>{descricao}</p>
                        </div>
                        <div style='text-align: right;'>
                            <span style='background-color: {cor_estado}; color: white; 
                                         padding: 3px 10px; border-radius: 15px; font-size: 0.8em;'>
                                {icone_estado} {estado}
                            </span>
                        </div>
                    </div>
                    <div style='margin-top: 10px; padding-top: 10px; border-top: 1px solid #dee2e6;'>
                        <span style='color: #666; font-size: 0.85em;'>
                            📅 <strong>Vigência:</strong> {periodo_inicio} a {periodo_fim if periodo_fim != 209912 else 'Indeterminado'}
                        </span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Resumo em tabela colapsável
    with st.expander("📋 Ver tabela completa de TTDs"):
        df_display = df_ttd.copy()
        df_display['periodo_fim'] = df_display['periodo_fim'].apply(
            lambda x: 'Indeterminado' if x == 209912 else str(x)
        )
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                'cd_beneficio': st.column_config.NumberColumn('Código', format='%d'),
                'de_beneficio': 'Descrição',
                'periodo_inicio': 'Início Vigência',
                'periodo_fim': 'Fim Vigência',
                'estado': 'Estado'
            }
        )
    
    # Alerta se houver muitos TTDs
    if total_ttds > 5:
        st.info(f"💡 Esta empresa possui {total_ttds} TTDs ativos. Verifique se todos os benefícios estão sendo aplicados corretamente nas operações.")


def render_tab_comparativo(dados: Dict):
    """Renderiza aba de comparativo Entradas vs Saídas."""
    
    # Dados de saída (NFe emitidas)
    df_nfe_saida = dados.get('nfe_emitidas_resumo', pd.DataFrame())
    df_ncm_saida = dados.get('top_ncm_nfe', pd.DataFrame())
    df_cfop_saida = dados.get('cfop_nfe', pd.DataFrame())
    
    # Dados de entrada (NFe recebidas)
    df_nfe_entrada = dados.get('nfe_recebidas_resumo', pd.DataFrame())
    df_ncm_entrada = dados.get('top_ncm_entrada', pd.DataFrame())
    df_cfop_entrada = dados.get('cfop_entrada', pd.DataFrame())
    
    # NFCe (só saída)
    df_nfce = dados.get('nfce_resumo', pd.DataFrame())
    
    # ===========================================
    # RESUMO GERAL
    # ===========================================
    st.markdown("### ⚖️ Resumo: Entradas vs Saídas")
    
    # Calcular totais
    total_entrada = df_nfe_entrada['valor_total'].sum() if not df_nfe_entrada.empty else 0
    total_saida_nfe = df_nfe_saida['valor_total'].sum() if not df_nfe_saida.empty else 0
    total_saida_nfce = df_nfce['valor_total'].sum() if not df_nfce.empty else 0
    total_saida = total_saida_nfe + total_saida_nfce
    
    saldo = total_saida - total_entrada
    margem_bruta = (saldo / total_entrada * 100) if total_entrada > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📥 Total Entradas", formatar_moeda(total_entrada))
    with col2:
        st.metric("📤 Total Saídas (NFe + NFCe)", formatar_moeda(total_saida))
    with col3:
        delta_color = "normal" if saldo >= 0 else "inverse"
        st.metric("💰 Saldo (Saídas - Entradas)", formatar_moeda(saldo), 
                  delta=f"{margem_bruta:.1f}% margem" if total_entrada > 0 else None,
                  delta_color=delta_color)
    with col4:
        qtd_entrada = df_nfe_entrada['qtd_notas'].sum() if not df_nfe_entrada.empty else 0
        qtd_saida = (df_nfe_saida['qtd_notas'].sum() if not df_nfe_saida.empty else 0) + \
                    (df_nfce['qtd_notas'].sum() if not df_nfce.empty else 0)
        st.metric("📋 Notas", f"{int(qtd_entrada)} ent. / {int(qtd_saida)} saí.")
    
    st.markdown("---")
    
    # ===========================================
    # COMPARATIVO MENSAL
    # ===========================================
    st.markdown("### 📊 Evolução Mensal")
    
    if not df_nfe_entrada.empty or not df_nfe_saida.empty:
        # Preparar dados
        df_entrada_mes = df_nfe_entrada[['periodo', 'valor_total']].copy() if not df_nfe_entrada.empty else pd.DataFrame(columns=['periodo', 'valor_total'])
        df_entrada_mes = df_entrada_mes.rename(columns={'valor_total': 'entrada'})
        df_entrada_mes['periodo'] = df_entrada_mes['periodo'].astype(str)
        
        df_saida_mes = df_nfe_saida[['periodo', 'valor_total']].copy() if not df_nfe_saida.empty else pd.DataFrame(columns=['periodo', 'valor_total'])
        df_saida_mes = df_saida_mes.rename(columns={'valor_total': 'saida_nfe'})
        df_saida_mes['periodo'] = df_saida_mes['periodo'].astype(str)
        
        # Merge
        df_comp = pd.merge(df_entrada_mes, df_saida_mes, on='periodo', how='outer').fillna(0)
        
        # Adicionar NFCe se houver
        if not df_nfce.empty:
            df_nfce_mes = df_nfce[['periodo', 'valor_total']].copy()
            df_nfce_mes = df_nfce_mes.rename(columns={'valor_total': 'saida_nfce'})
            df_nfce_mes['periodo'] = df_nfce_mes['periodo'].astype(str)
            df_comp = pd.merge(df_comp, df_nfce_mes, on='periodo', how='outer').fillna(0)
        else:
            df_comp['saida_nfce'] = 0
        
        df_comp['saida_total'] = df_comp['saida_nfe'] + df_comp['saida_nfce']
        df_comp['saldo'] = df_comp['saida_total'] - df_comp['entrada']
        df_comp = df_comp.sort_values('periodo')
        
        # Gráfico
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_comp['periodo'],
            y=df_comp['entrada'],
            name='Entradas',
            marker_color='#dc3545'
        ))
        fig.add_trace(go.Bar(
            x=df_comp['periodo'],
            y=df_comp['saida_total'],
            name='Saídas',
            marker_color='#28a745'
        ))
        fig.add_trace(go.Scatter(
            x=df_comp['periodo'],
            y=df_comp['saldo'],
            name='Saldo',
            mode='lines+markers',
            line=dict(color='#1e3c72', width=3),
            yaxis='y2'
        ))
        fig.update_layout(
            title='Entradas vs Saídas por Período',
            xaxis_title='Período',
            xaxis={'type': 'category'},
            yaxis=dict(title='Valor (R$)', side='left'),
            yaxis2=dict(title='Saldo (R$)', side='right', overlaying='y'),
            barmode='group',
            height=400,
            legend=dict(orientation='h', y=-0.15)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # ===========================================
    # COMPARATIVO NCM
    # ===========================================
    st.markdown("### 📦 Comparativo por NCM")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📥 Top NCM Entradas")
        if not df_ncm_entrada.empty:
            df_display = df_ncm_entrada.copy()
            if 'descricao_ncm' in df_display.columns:
                df_display['descricao_ncm'] = df_display['descricao_ncm'].apply(
                    lambda x: x[:40] + '...' if len(str(x)) > 40 else x
                )
            max_valor = df_display['valor_total'].max()
            
            st.dataframe(
                df_display[['ncm', 'descricao_ncm', 'valor_total', 'qtd_notas']] if 'descricao_ncm' in df_display.columns else df_display[['ncm', 'valor_total', 'qtd_notas']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'ncm': 'NCM',
                    'descricao_ncm': 'Descrição',
                    'valor_total': col_barra_valor('Valor', max_valor),
                    'qtd_notas': col_numero('Notas')
                }
            )
        else:
            st.info("Sem dados de NCM de entrada.")
    
    with col2:
        st.markdown("#### 📤 Top NCM Saídas")
        if not df_ncm_saida.empty:
            df_display = df_ncm_saida.copy()
            if 'descricao_ncm' in df_display.columns:
                df_display['descricao_ncm'] = df_display['descricao_ncm'].apply(
                    lambda x: x[:40] + '...' if len(str(x)) > 40 else x
                )
            max_valor = df_display['valor_total'].max()
            
            st.dataframe(
                df_display[['ncm', 'descricao_ncm', 'valor_total', 'qtd_notas']] if 'descricao_ncm' in df_display.columns else df_display[['ncm', 'valor_total', 'qtd_notas']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'ncm': 'NCM',
                    'descricao_ncm': 'Descrição',
                    'valor_total': col_barra_valor('Valor', max_valor),
                    'qtd_notas': col_numero('Notas')
                }
            )
        else:
            st.info("Sem dados de NCM de saída.")
    
    # ===========================================
    # NCMs EM COMUM (Análise de Markup)
    # ===========================================
    if not df_ncm_entrada.empty and not df_ncm_saida.empty:
        st.markdown("---")
        st.markdown("### 💹 Análise de Markup por NCM")
        st.caption("NCMs que aparecem tanto nas entradas quanto nas saídas")
        
        # Merge por NCM
        df_entrada_ncm = df_ncm_entrada[['ncm', 'valor_total', 'qtd_itens']].copy()
        df_entrada_ncm = df_entrada_ncm.rename(columns={'valor_total': 'valor_entrada', 'qtd_itens': 'qtd_entrada'})
        
        df_saida_ncm = df_ncm_saida[['ncm', 'valor_total', 'qtd_itens']].copy()
        df_saida_ncm = df_saida_ncm.rename(columns={'valor_total': 'valor_saida', 'qtd_itens': 'qtd_saida'})
        
        if 'descricao_ncm' in df_ncm_saida.columns:
            df_saida_ncm['descricao_ncm'] = df_ncm_saida['descricao_ncm']
        
        df_markup = pd.merge(df_entrada_ncm, df_saida_ncm, on='ncm', how='inner')
        
        if not df_markup.empty:
            # Calcular markup
            df_markup['markup_valor'] = df_markup['valor_saida'] - df_markup['valor_entrada']
            df_markup['markup_pct'] = np.where(
                df_markup['valor_entrada'] > 0,
                (df_markup['markup_valor'] / df_markup['valor_entrada']) * 100,
                0
            )
            df_markup = df_markup.sort_values('markup_valor', ascending=False)
            
            # Truncar descrição
            if 'descricao_ncm' in df_markup.columns:
                df_markup['descricao_ncm'] = df_markup['descricao_ncm'].apply(
                    lambda x: x[:40] + '...' if len(str(x)) > 40 else x
                )
            
            max_entrada = df_markup['valor_entrada'].max()
            max_saida = df_markup['valor_saida'].max()
            
            colunas = ['ncm', 'valor_entrada', 'valor_saida', 'markup_valor', 'markup_pct']
            if 'descricao_ncm' in df_markup.columns:
                colunas.insert(1, 'descricao_ncm')
            
            st.dataframe(
                df_markup[colunas],
                use_container_width=True,
                hide_index=True,
                column_config={
                    'ncm': 'NCM',
                    'descricao_ncm': 'Descrição',
                    'valor_entrada': col_barra_valor('Entrada', max_entrada),
                    'valor_saida': col_barra_valor('Saída', max_saida),
                    'markup_valor': col_moeda('Markup R$'),
                    'markup_pct': col_percentual('Markup %')
                }
            )
            
            # Alertas
            ncms_negativos = df_markup[df_markup['markup_pct'] < 0]
            if not ncms_negativos.empty:
                st.warning(f"⚠️ {len(ncms_negativos)} NCM(s) com markup negativo (vendendo por menos do que compra)")
        else:
            st.info("Não há NCMs em comum entre entradas e saídas para análise de markup.")
    
    st.markdown("---")
    
    # ===========================================
    # COMPARATIVO CFOP
    # ===========================================
    st.markdown("### 📋 Resumo por Tipo de Operação (CFOP)")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📥 CFOPs de Entrada")
        if not df_cfop_entrada.empty:
            # Agrupar por tipo
            def classificar_cfop_entrada(cfop):
                try:
                    cfop = int(cfop)
                    if cfop >= 1000 and cfop < 2000:
                        return "Interna (1XXX)"
                    elif cfop >= 2000 and cfop < 3000:
                        return "Interestadual (2XXX)"
                    elif cfop >= 3000 and cfop < 4000:
                        return "Exterior (3XXX)"
                    else:
                        return "Outros"
                except:
                    return "Outros"
            
            df_cfop_entrada['tipo'] = df_cfop_entrada['cfop'].apply(classificar_cfop_entrada)
            df_resumo = df_cfop_entrada.groupby('tipo').agg({
                'valor_total': 'sum',
                'qtd_notas': 'sum'
            }).reset_index()
            df_resumo = df_resumo.sort_values('valor_total', ascending=False)
            max_valor = df_resumo['valor_total'].max()
            
            st.dataframe(
                df_resumo,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'tipo': 'Tipo Operação',
                    'valor_total': col_barra_valor('Valor', max_valor),
                    'qtd_notas': col_numero('Notas')
                }
            )
        else:
            st.info("Sem dados de CFOP de entrada.")
    
    with col2:
        st.markdown("#### 📤 CFOPs de Saída")
        if not df_cfop_saida.empty:
            # Agrupar por tipo
            def classificar_cfop_saida(cfop):
                try:
                    cfop = int(cfop)
                    if cfop >= 5000 and cfop < 6000:
                        return "Interna (5XXX)"
                    elif cfop >= 6000 and cfop < 7000:
                        return "Interestadual (6XXX)"
                    elif cfop >= 7000:
                        return "Exterior (7XXX)"
                    else:
                        return "Outros"
                except:
                    return "Outros"
            
            df_cfop_saida['tipo'] = df_cfop_saida['cfop'].apply(classificar_cfop_saida)
            df_resumo = df_cfop_saida.groupby('tipo').agg({
                'valor_total': 'sum',
                'qtd_notas': 'sum'
            }).reset_index()
            df_resumo = df_resumo.sort_values('valor_total', ascending=False)
            max_valor = df_resumo['valor_total'].max()
            
            st.dataframe(
                df_resumo,
                use_container_width=True,
                hide_index=True,
                column_config={
                    'tipo': 'Tipo Operação',
                    'valor_total': col_barra_valor('Valor', max_valor),
                    'qtd_notas': col_numero('Notas')
                }
            )
        else:
            st.info("Sem dados de CFOP de saída.")


def render_tab_cfop(dados: Dict):
    """Renderiza aba de análise por CFOP - apenas métricas e tabela."""
    
    df_cfop = dados.get('cfop_nfe', pd.DataFrame())
    
    if df_cfop.empty:
        st.warning("""
        ⚠️ **Dados de CFOP não encontrados**
        
        Não foram encontrados dados de CFOP para o período selecionado.
        Isso pode ocorrer se não houver NFe emitidas no período ou se houver erro na consulta.
        """)
        return
    
    st.markdown("### 📋 Distribuição por CFOP")
    
    # Usar classificação da tabela se disponível, senão calcular
    if 'entrada_saida' in df_cfop.columns and df_cfop['entrada_saida'].notna().any():
        # Usar dados da tabela de CFOP
        df_cfop['tipo_operacao'] = df_cfop.apply(
            lambda x: f"{x['entrada_saida']} {x['local_operacao']}" if pd.notna(x['entrada_saida']) else 'Outros', 
            axis=1
        )
    else:
        # Classificar manualmente
        def classificar_cfop(cfop):
            try:
                cfop = int(cfop)
                if cfop >= 1000 and cfop < 2000:
                    return "Entrada Interna"
                elif cfop >= 2000 and cfop < 3000:
                    return "Entrada Interestadual"
                elif cfop >= 3000 and cfop < 4000:
                    return "Entrada Exterior"
                elif cfop >= 5000 and cfop < 6000:
                    return "Saída Interna"
                elif cfop >= 6000 and cfop < 7000:
                    return "Saída Interestadual"
                elif cfop >= 7000:
                    return "Saída Exterior"
                else:
                    return "Outros"
            except:
                return "Outros"
        df_cfop['tipo_operacao'] = df_cfop['cfop'].apply(classificar_cfop)
    
    # Métricas resumo
    col1, col2, col3, col4 = st.columns(4)
    
    total_valor = df_cfop['valor_total'].sum()
    total_icms = df_cfop['valor_icms'].sum() if 'valor_icms' in df_cfop.columns else 0
    
    # Calcular por tipo (usando classificação manual para consistência)
    def get_tipo_simples(cfop):
        try:
            cfop = int(cfop)
            if cfop >= 5000 and cfop < 6000:
                return "Saída Interna"
            elif cfop >= 6000 and cfop < 7000:
                return "Saída Interestadual"
            elif cfop >= 7000:
                return "Saída Exterior"
            else:
                return "Outros"
        except:
            return "Outros"
    
    df_cfop['tipo_simples'] = df_cfop['cfop'].apply(get_tipo_simples)
    saidas_internas = df_cfop[df_cfop['tipo_simples'] == 'Saída Interna']['valor_total'].sum()
    saidas_interestaduais = df_cfop[df_cfop['tipo_simples'] == 'Saída Interestadual']['valor_total'].sum()
    saidas_exterior = df_cfop[df_cfop['tipo_simples'] == 'Saída Exterior']['valor_total'].sum()
    
    with col1:
        st.metric("Saídas Internas (5XXX)", formatar_moeda(saidas_internas))
    with col2:
        st.metric("Saídas Interestaduais (6XXX)", formatar_moeda(saidas_interestaduais))
    with col3:
        st.metric("Exportações (7XXX)", formatar_moeda(saidas_exterior))
    with col4:
        st.metric("ICMS Total dos Itens", formatar_moeda(total_icms))
    
    st.markdown("---")
    
    # Tabela completa com descrições
    st.markdown("### 📊 Detalhamento por CFOP")
    
    df_display = df_cfop.copy()
    total = df_display['valor_total'].sum()
    df_display['participacao'] = df_display['valor_total'] / total * 100
    
    # Truncar descrição se existir
    if 'descricao_cfop' in df_display.columns:
        df_display['descricao_cfop'] = df_display['descricao_cfop'].apply(
            lambda x: x[:50] + '...' if len(str(x)) > 50 else x
        )
    
    # Max para barras
    max_valor = df_display['valor_total'].max()
    max_icms = df_display['valor_icms'].max() if 'valor_icms' in df_display.columns else 1
    
    # Selecionar colunas para exibição
    colunas = ['cfop', 'qtd_notas', 'qtd_itens', 'valor_total', 'participacao']
    config = {
        'cfop': 'CFOP',
        'qtd_notas': col_numero('Notas'),
        'qtd_itens': col_numero('Itens'),
        'valor_total': col_barra_valor('Valor Total', max_valor),
        'participacao': col_barra_pct('% Part.')
    }
    
    if 'descricao_cfop' in df_display.columns:
        colunas.insert(1, 'descricao_cfop')
        config['descricao_cfop'] = 'Descrição'
    
    if 'valor_icms' in df_display.columns:
        colunas.insert(-1, 'valor_icms')
        config['valor_icms'] = col_barra_valor('ICMS', max_icms)
    
    st.dataframe(
        df_display[colunas],
        use_container_width=True,
        hide_index=True,
        column_config=config
    )


# =============================================================================
# FUNÇÃO PRINCIPAL DE BUSCA
# =============================================================================

def buscar_dados_empresa_com_progresso(cnpj: str = None, ie: str = None, periodo_inicio: int = None, periodo_fim: int = None) -> Dict:
    """Busca dados da empresa com barra de progresso visual e logs detalhados."""
    
    if not periodo_inicio or not periodo_fim:
        periodo_inicio, periodo_fim = calcular_periodo_default()
    
    dados = {}
    
    # Usar placeholder que pode ser completamente limpo
    progress_placeholder = st.empty()
    
    with progress_placeholder.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("""
            <div style='text-align: center; padding: 20px;'>
                <h3>🔄 Carregando dados...</h3>
            </div>
            """, unsafe_allow_html=True)
            
            progress_bar = st.progress(0)
            tempo_text = st.empty()

            total_etapas = 17
            etapa_atual = 0
            tempo_inicio = time.time()
            tempo_etapa_inicio = time.time()
            tempos_etapas = []

            def atualizar_progresso(mensagem: str):
                nonlocal etapa_atual, tempo_etapa_inicio

                # Calcular tempo da etapa anterior
                if etapa_atual > 0:
                    tempo_etapa = time.time() - tempo_etapa_inicio
                    tempos_etapas.append(tempo_etapa)

                etapa_atual += 1
                tempo_etapa_inicio = time.time()

                # Calcular porcentagem
                pct = int((etapa_atual / total_etapas) * 100)

                # Calcular tempo decorrido
                tempo_decorrido = int(time.time() - tempo_inicio)

                # Estimar tempo restante (média simples)
                if tempos_etapas:
                    tempo_medio = sum(tempos_etapas) / len(tempos_etapas)
                    etapas_restantes = total_etapas - etapa_atual
                    tempo_restante = int(tempo_medio * etapas_restantes)

                    if tempo_restante >= 60:
                        tempo_restante_texto = f"~{tempo_restante // 60}min {tempo_restante % 60}s"
                    else:
                        tempo_restante_texto = f"~{tempo_restante}s"

                    tempo_texto = f"⏱️ {tempo_decorrido}s decorridos | {tempo_restante_texto} restantes"
                else:
                    tempo_texto = "⏱️ Calculando tempo..."

                # Atualizar UI
                progress_bar.progress(pct / 100, text=f"{mensagem} ({pct}%)")
                tempo_text.markdown(f"<p style='text-align: center; color: #666;'>{tempo_texto}</p>", unsafe_allow_html=True)
            
            # Etapa 1: Cadastro
            atualizar_progresso("📋 Buscando dados cadastrais...")
            df_cadastro = executar_query_cached(
                NotasQueries.get_cadastro_query(ie=ie, cnpj=cnpj),
                _cache_key=f"cadastro_{cnpj}_{ie}"
            )
            if df_cadastro.empty:
                progress_placeholder.empty()
                return None
            dados['cadastro'] = df_cadastro.iloc[0].to_dict()
            
            cnpj_limpo = limpar_cnpj(dados['cadastro'].get('cnpj', ''))
            ie_empresa = dados['cadastro'].get('inscricao_estadual', '')
            cnae = dados['cadastro'].get('cnae', '')
            cache_prefix = f"{cnpj_limpo}_{periodo_inicio}_{periodo_fim}"
            
            # Etapa 2: NFe Emitidas
            atualizar_progresso("📤 Buscando NFe emitidas...")
            dados['nfe_emitidas_resumo'] = executar_query_cached(
                NotasQueries.get_nfe_emitidas_resumo(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"nfe_emit_{cache_prefix}"
            )
            
            # Etapa 3: NFe Recebidas
            atualizar_progresso("📥 Buscando NFe recebidas...")
            dados['nfe_recebidas_resumo'] = executar_query_cached(
                NotasQueries.get_nfe_recebidas_resumo(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"nfe_receb_{cache_prefix}"
            )
            
            # Etapa 4: NFCe
            atualizar_progresso("🛒 Buscando NFCe...")
            dados['nfce_resumo'] = executar_query_cached(
                NotasQueries.get_nfce_resumo(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"nfce_{cache_prefix}"
            )
            
            # Etapa 5: Top Clientes
            atualizar_progresso("👥 Buscando top clientes...")
            dados['top_clientes'] = executar_query_cached(
                NotasQueries.get_top_clientes(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"clientes_{cache_prefix}"
            )
            
            # Etapa 6: Top Fornecedores
            atualizar_progresso("🏭 Buscando top fornecedores...")
            dados['top_fornecedores'] = executar_query_cached(
                NotasQueries.get_top_fornecedores(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"fornec_{cache_prefix}"
            )
            
            # Etapa 7: NCM NFe
            atualizar_progresso("📦 Buscando NCM NFe...")
            dados['top_ncm_nfe'] = executar_query_cached(
                NotasQueries.get_top_ncm_nfe(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"ncm_nfe_{cache_prefix}"
            )
            
            # Etapa 8: NCM NFCe
            atualizar_progresso("📦 Buscando NCM NFCe...")
            dados['top_ncm_nfce'] = executar_query_cached(
                NotasQueries.get_top_ncm_nfce(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"ncm_nfce_{cache_prefix}"
            )
            
            # Etapa 9: Produtos NFe
            atualizar_progresso("🏷️ Buscando produtos NFe...")
            dados['top_produtos_nfe'] = executar_query_cached(
                NotasQueries.get_top_produtos_nfe(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"prod_nfe_{cache_prefix}"
            )
            
            # Etapa 10: Produtos NFCe
            atualizar_progresso("🏷️ Buscando produtos NFCe...")
            dados['top_produtos_nfce'] = executar_query_cached(
                NotasQueries.get_top_produtos_nfce(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"prod_nfce_{cache_prefix}"
            )
            
            # Etapa 11: CFOP
            atualizar_progresso("📋 Buscando CFOP...")
            dados['cfop_nfe'] = executar_query_cached(
                NotasQueries.get_cfop_nfe(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"cfop_{cache_prefix}"
            )
            
            # Etapa 12: Tributação
            atualizar_progresso("💹 Buscando tributação...")
            dados['tributacao_nfe'] = executar_query_cached(
                NotasQueries.get_tributacao_nfe(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"trib_{cache_prefix}"
            )
            
            # Etapa 13: NCM Entrada (para comparativo)
            atualizar_progresso("📥 Buscando NCM de entradas...")
            dados['top_ncm_entrada'] = executar_query_cached(
                NotasQueries.get_top_ncm_entrada(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"ncm_entrada_{cache_prefix}"
            )
            
            # Etapa 14: CFOP Entrada (para comparativo)
            atualizar_progresso("📥 Buscando CFOP de entradas...")
            dados['cfop_entrada'] = executar_query_cached(
                NotasQueries.get_cfop_entrada(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"cfop_entrada_{cache_prefix}"
            )
            
            # Etapa 15: Faturamento DIME
            atualizar_progresso("💰 Buscando faturamento DIME...")
            dados['faturamento_dime'] = executar_query_cached(
                NotasQueries.get_faturamento_dime(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"dime_{cache_prefix}"
            )
            
            # Etapa 16: Faturamento PGDAS
            atualizar_progresso("💰 Buscando faturamento PGDAS...")
            dados['faturamento_pgdas'] = executar_query_cached(
                NotasQueries.get_faturamento_pgdas(cnpj_limpo, periodo_inicio, periodo_fim),
                _cache_key=f"pgdas_{cache_prefix}"
            )
            
            # Etapa 17: TTDs
            atualizar_progresso("🎫 Buscando TTDs...")
            dados['ttd_empresa'] = executar_query_cached(
                NotasQueries.get_ttd_empresa(ie_empresa),
                _cache_key=f"ttd_{ie_empresa}"
            )
            
            # Estatísticas do Setor (extra, não conta no progresso)
            if cnae:
                dados['setor_stats'] = executar_query_cached(
                    NotasQueries.get_setor_stats(cnae, periodo_inicio, periodo_fim),
                    _cache_key=f"setor_{cnae}_{periodo_inicio}_{periodo_fim}"
                )
            else:
                dados['setor_stats'] = pd.DataFrame()
            
            # Calcular métricas
            metricas = {
                'nfe_emitidas_qtd': dados['nfe_emitidas_resumo']['qtd_notas'].sum() if not dados['nfe_emitidas_resumo'].empty else 0,
                'nfe_emitidas_valor': dados['nfe_emitidas_resumo']['valor_total'].sum() if not dados['nfe_emitidas_resumo'].empty else 0,
                'nfe_recebidas_qtd': dados['nfe_recebidas_resumo']['qtd_notas'].sum() if not dados['nfe_recebidas_resumo'].empty else 0,
                'nfe_recebidas_valor': dados['nfe_recebidas_resumo']['valor_total'].sum() if not dados['nfe_recebidas_resumo'].empty else 0,
                'nfce_qtd': dados['nfce_resumo']['qtd_notas'].sum() if not dados['nfce_resumo'].empty else 0,
                'nfce_valor': dados['nfce_resumo']['valor_total'].sum() if not dados['nfce_resumo'].empty else 0,
            }
            dados['metricas'] = metricas
    
    # Limpar completamente o placeholder de progresso
    progress_placeholder.empty()
    
    return dados


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Função principal do aplicativo."""
    
    # Título - só mostra se não houver dados carregados
    if 'dados' not in st.session_state:
        st.markdown("""
        <div style='text-align: center; padding: 20px 0;'>
            <h1 style='color: #1e3c72;'>📄 SISTEMA NOTAS</h1>
            <p style='color: #666;'>Análise de Notas Fiscais Eletrônicas (NFe/NFCe)</p>
        </div>
        """, unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🔍 Buscar Empresa")
        
        tipo_busca = st.radio("Buscar por:", ["CNPJ", "Inscrição Estadual"], horizontal=True)
        
        if tipo_busca == "CNPJ":
            cnpj_input = st.text_input("CNPJ:", placeholder="00.000.000/0000-00")
            ie_input = None
        else:
            ie_input = st.text_input("Inscrição Estadual:", placeholder="000000000")
            cnpj_input = None
        
        st.markdown("---")
        st.markdown("### 📅 Período de Análise")
        
        periodo_inicio_default, periodo_fim_default = calcular_periodo_default()

        # Extrair ano e mês dos valores padrão
        ano_inicio_default = periodo_inicio_default // 100
        mes_inicio_default = periodo_inicio_default % 100
        ano_fim_default = periodo_fim_default // 100
        mes_fim_default = periodo_fim_default % 100

        # Lista de anos disponíveis (dinâmica baseada no período)
        anos_disponiveis = sorted(set([ano_inicio_default, ano_fim_default, 2024, 2025, datetime.now().year]))

        col1, col2 = st.columns(2)
        with col1:
            ano_inicio = st.selectbox("Ano Início:", anos_disponiveis, index=anos_disponiveis.index(ano_inicio_default))
            mes_inicio = st.selectbox("Mês Início:", range(1, 13), index=mes_inicio_default - 1)
        with col2:
            ano_fim = st.selectbox("Ano Fim:", anos_disponiveis, index=anos_disponiveis.index(ano_fim_default))
            mes_fim = st.selectbox("Mês Fim:", range(1, 13), index=mes_fim_default - 1)
        
        periodo_inicio = ano_inicio * 100 + mes_inicio
        periodo_fim = ano_fim * 100 + mes_fim
        
        st.markdown("---")
        
        buscar = st.button("🔍 Buscar", type="primary", use_container_width=True)
        
        if st.button("🔄 Limpar Cache", use_container_width=True):
            st.cache_data.clear()
            st.success("Cache limpo!")
        
        # Botão para nova consulta (limpar dados)
        if 'dados' in st.session_state:
            if st.button("🔄 Nova Consulta", use_container_width=True):
                del st.session_state['dados']
                st.rerun()
    
    # Conteúdo principal
    if buscar:
        cnpj = limpar_cnpj(cnpj_input) if cnpj_input else None
        ie = limpar_ie(ie_input) if ie_input else None
        
        if not cnpj and not ie:
            st.error("❌ Informe um CNPJ ou Inscrição Estadual.")
            return
        
        # Marcar que está iniciando nova busca e limpar dados antigos
        st.session_state['buscando'] = True
        if 'dados' in st.session_state:
            del st.session_state['dados']
    
    # Se está no meio de uma busca, executar
    if st.session_state.get('buscando', False):
        cnpj = limpar_cnpj(cnpj_input) if cnpj_input else None
        ie = limpar_ie(ie_input) if ie_input else None
        
        dados = buscar_dados_empresa_com_progresso(cnpj=cnpj, ie=ie, periodo_inicio=periodo_inicio, periodo_fim=periodo_fim)
        
        # Limpar flag de busca
        st.session_state['buscando'] = False
        
        if not dados:
            st.error("❌ Empresa não encontrada.")
            return
        
        st.session_state['dados'] = dados
        st.session_state['periodo_inicio'] = periodo_inicio
        st.session_state['periodo_fim'] = periodo_fim
        st.rerun()  # Rerun para aplicar o layout limpo
    
    # Exibir dados se disponíveis
    if 'dados' in st.session_state:
        dados = st.session_state['dados']
        cadastro = dados.get('cadastro', {})
        periodo_inicio = st.session_state.get('periodo_inicio', 202401)
        periodo_fim = st.session_state.get('periodo_fim', 202512)
        
        # =========================================================================
        # CSS PARA SIDEBAR SEMPRE COLAPSADO (após dados carregados)
        # =========================================================================
        st.markdown("""
        <style>
            /* Sidebar sempre colapsado por padrão */
            section[data-testid="stSidebar"] {
                width: 0px !important;
                min-width: 0px !important;
                transform: translateX(-100%);
                transition: transform 0.3s ease-in-out, width 0.3s ease-in-out;
            }
            section[data-testid="stSidebar"]:hover,
            section[data-testid="stSidebar"]:focus-within {
                width: 300px !important;
                min-width: 300px !important;
                transform: translateX(0);
            }
            /* Indicador visual para expandir */
            section[data-testid="stSidebar"]::before {
                content: "☰";
                position: absolute;
                right: -30px;
                top: 50%;
                transform: translateY(-50%);
                font-size: 24px;
                color: #1e3c72;
                cursor: pointer;
                z-index: 1000;
            }
        </style>
        """, unsafe_allow_html=True)
        
        render_header(cadastro)
        
        # Abas
        tabs = st.tabs([
            "📋 Cadastro",
            "📊 Visão Geral",
            "⚖️ Entradas vs Saídas",
            "📦 Produtos NFe",
            "🛒 Produtos NFCe",
            "👥 Clientes",
            "🏭 Fornecedores",
            "💰 Faturamento",
            "💹 Tributação",
            "📋 CFOP",
            "🎯 Setor",
            "🎫 TTDs"
        ])
        
        with tabs[0]:
            render_tab_cadastro(cadastro)
        
        with tabs[1]:
            render_tab_visao_geral(dados, periodo_inicio, periodo_fim)
        
        with tabs[2]:
            render_tab_comparativo(dados)
        
        with tabs[3]:
            render_tab_produtos(dados, tipo='nfe')
        
        with tabs[4]:
            render_tab_produtos(dados, tipo='nfce')
        
        with tabs[5]:
            render_tab_clientes(dados)
        
        with tabs[6]:
            render_tab_fornecedores(dados)
        
        with tabs[7]:
            render_tab_faturamento(dados, cadastro)
        
        with tabs[8]:
            render_tab_tributacao(dados)
        
        with tabs[9]:
            render_tab_cfop(dados)
        
        with tabs[10]:
            render_tab_setor(dados, cadastro)
        
        with tabs[11]:
            render_tab_ttd(dados, cadastro)
    
    else:
        st.info("👈 Use a barra lateral para buscar uma empresa.")


if __name__ == "__main__":
    main()