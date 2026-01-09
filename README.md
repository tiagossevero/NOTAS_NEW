# SISTEMA NOTAS - Análise de Notas Fiscais Eletrônicas (NFe/NFCe)

Sistema para análise de notas fiscais eletrônicas com comparação setorial, desenvolvido pela SEF/SC - Secretaria de Estado da Fazenda de Santa Catarina.

## Sobre o Projeto

O **Sistema NOTAS** é uma aplicação web desenvolvida em Python com Streamlit que permite a análise detalhada de Notas Fiscais Eletrônicas (NFe) e Notas Fiscais de Consumidor Eletrônicas (NFCe). O sistema conecta-se a um banco de dados Impala para extrair e processar informações fiscais de contribuintes do estado de Santa Catarina.

## Funcionalidades

### Busca de Empresas
- Busca por **CNPJ** ou **Inscrição Estadual**
- Seleção de período de análise personalizado (mês/ano)
- Cache de dados para otimização de performance

### Abas de Análise

| Aba | Descrição |
|-----|-----------|
| **Cadastro** | Dados cadastrais completos do contribuinte (razão social, CNPJ, IE, CNAE, regime de apuração, endereço, contador, etc.) |
| **Visão Geral** | Dashboard com KPIs principais e evolução temporal de NFe emitidas, recebidas e NFCe |
| **Entradas vs Saídas** | Comparativo entre operações de entrada e saída com análise por CFOP |
| **Produtos NFe** | Top produtos comercializados via NFe, análise por NCM com descrições |
| **Produtos NFCe** | Top produtos comercializados via NFCe para varejo |
| **Clientes** | Ranking dos principais clientes por volume de vendas |
| **Fornecedores** | Ranking dos principais fornecedores por volume de compras |
| **Faturamento** | Análise de faturamento com dados de DIME e PGDAS (Simples Nacional) |
| **Tributação** | Análise detalhada da tributação: CST, alíquotas de ICMS, valores de imposto |
| **CFOP** | Distribuição de operações por CFOP com classificação por tipo |
| **Setor** | Benchmark setorial: comparação com empresas do mesmo CNAE |
| **TTDs** | Tratamentos Tributários Diferenciados vigentes da empresa |

### Análises Disponíveis

- **NFe Emitidas**: Notas fiscais de saída (vendas)
- **NFe Recebidas**: Notas fiscais de entrada (compras)
- **NFCe**: Notas fiscais de consumidor (varejo)
- **Análise por NCM**: Classificação fiscal de mercadorias
- **Análise por CFOP**: Código fiscal de operações e prestações
- **Indicadores de Concentração**: Índice HHI para análise de concentração de clientes/fornecedores
- **Detecção de Outliers**: Identificação de valores atípicos usando método IQR
- **Benchmark Setorial**: Comparação com médias do setor (CNAE)

## Tecnologias Utilizadas

| Tecnologia | Versão | Uso |
|------------|--------|-----|
| Python | 3.x | Linguagem principal |
| Streamlit | - | Framework de interface web |
| Pandas | - | Manipulação e análise de dados |
| NumPy | - | Operações numéricas |
| Plotly | - | Visualizações interativas (gráficos) |
| SQLAlchemy | - | Conexão com banco de dados |
| Impala | - | Banco de dados (Apache Impala) |

## Pré-requisitos

- Python 3.8 ou superior
- Acesso ao banco de dados Impala da SEF/SC
- Credenciais de autenticação LDAP

## Instalação

1. Clone o repositório:
```bash
git clone <url-do-repositorio>
cd NOTAS_NEW
```

2. Crie um ambiente virtual (recomendado):
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Instale as dependências:
```bash
pip install streamlit pandas numpy plotly sqlalchemy impyla python-dateutil
```

## Configuração

### Credenciais do Banco de Dados

O sistema utiliza o **Streamlit Secrets** para gerenciar credenciais de forma segura. Crie um arquivo `.streamlit/secrets.toml` com o seguinte conteúdo:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

> **Importante**: Nunca versione o arquivo `secrets.toml`. Adicione-o ao `.gitignore`.

### Configuração de Conexão

O sistema se conecta ao Impala através dos seguintes parâmetros (já configurados no código):

- **Host**: `bdaworkernode02.sef.sc.gov.br`
- **Porta**: `21050`
- **Autenticação**: LDAP com SSL

## Como Executar

Execute o aplicativo com o Streamlit:

```bash
streamlit run "NOTAS (1).py"
```

O sistema será iniciado e estará disponível em `http://localhost:8501`.

## Estrutura do Código

```
NOTAS_NEW/
├── NOTAS (1).py      # Arquivo principal da aplicação
├── README.md         # Documentação do projeto
└── .streamlit/
    └── secrets.toml  # Credenciais (não versionado)
```

### Organização do Código Principal

O arquivo `NOTAS (1).py` está organizado nas seguintes seções:

| Seção | Descrição |
|-------|-----------|
| **Configuração** | Imports, configuração SSL e setup da página Streamlit |
| **Banco de Dados** | Funções de conexão e execução de queries no Impala |
| **Funções Auxiliares** | Formatação de CNPJ, IE, moeda, percentual, etc. |
| **Configurações de Colunas** | Helpers para tabelas do Streamlit com formatação BR |
| **NotasQueries** | Classe com todas as queries SQL do sistema |
| **Análises Estatísticas** | Funções de concentração, outliers e variação |
| **Renderização** | Funções `render_tab_*` para cada aba da interface |
| **Main** | Função principal com sidebar e lógica de navegação |

## Queries SQL

O sistema utiliza queries SQL complexas que acessam:

- `nfe.nfe` - Tabela de Notas Fiscais Eletrônicas
- `nfce.nfce` - Tabela de Notas Fiscais de Consumidor
- `usr_sat_ods.vw_ods_contrib` - View de dados cadastrais
- `niat.tabela_ncm` - Tabela de descrições NCM
- `niat.tabela_cfop` - Tabela de descrições CFOP

### Observação sobre Arrays no Impala

O sistema utiliza a sintaxe específica do Impala para acessar campos dentro de arrays (itens das notas):

```sql
SELECT det.item.prod.ncm, det.item.prod.vprod
FROM nfe.nfe a, a.procnfe.nfe.infnfe.det det
```

## Cache e Performance

- **Cache de Engine**: Conexão Impala é cacheada com `@st.cache_resource`
- **Cache de Queries**: Dados são cacheados por 10 minutos com `@st.cache_data(ttl=600)`
- **Execução Paralela**: Queries são executadas em paralelo usando `ThreadPoolExecutor`
- **Botão Limpar Cache**: Disponível na sidebar para forçar atualização dos dados

## Uso do Sistema

1. Acesse a aplicação pelo navegador
2. Na **barra lateral**, selecione o tipo de busca (CNPJ ou IE)
3. Informe o número do documento
4. Selecione o **período de análise** (mês/ano início e fim)
5. Clique em **Buscar**
6. Navegue pelas **abas** para visualizar as diferentes análises
7. Use **Nova Consulta** para pesquisar outra empresa

## Visualizações

O sistema inclui diversos tipos de gráficos interativos:

- **Gráficos de Linha**: Evolução temporal de valores
- **Gráficos de Barras**: Comparativos e rankings
- **Gráficos de Pizza/Rosca**: Distribuição percentual
- **Gráficos de Área**: Evolução com preenchimento
- **Subplots**: Múltiplos gráficos relacionados
- **Indicadores de Gauge**: Para métricas de benchmark

## Autor

**SEF/SC - Secretaria de Estado da Fazenda de Santa Catarina**

## Licença

Este projeto é de uso interno da Secretaria de Estado da Fazenda de Santa Catarina.

---

*Documentação gerada em Janeiro/2026*
