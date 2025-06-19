import os
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text, types
from dotenv import load_dotenv

# -- Configuração Inicial --
#configura o logging para exibir mensagens informativas sobre a execução do pipeline.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

#Carrega as variáveis d ambiente (credenciais do DB - arquivo .env) 
load_dotenv()

# -- Constantes do Projeto --
API_URL_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
RAW_DATA_PATH = "dados_brutos_municipios.csv"
ENRICHMENT_DATA_PATH = "regioes_enriquecimento.csv"
DB_TABLE_NAME = "municipios_brasil"


def extrair_dados() -> pd.DataFrame | None:
    """
    Etapa 1: Extração
    Extrai dados da API de municípios do IBGE e salva uma cópia bruta.
    """
    logging.info("Iniciando extração de dados da API do IBGE...")
    try:
        response = requests.get(API_URL_MUNICIPIOS, timeout=30)
        response.raise_for_status()  # Lança exceção para status de erro (4xx ou 5xx)
        
        data = response.json()
        df = pd.DataFrame(data)
        
        # Salva uma cópia dos dados brutos como backup
        df.to_csv(RAW_DATA_PATH, index=False, encoding='utf-8')
        logging.info(f"Dados brutos salvos com sucesso em '{RAW_DATA_PATH}'.")
        
        return df

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar a API do IBGE: {e}")
        return None
    
def explorar_dados(df: pd.DataFrame):
    """
    Etapa 2: Exploração
    Realiza uma análise exploratória inicial nos dados.
    """
    logging.info("Iniciando exploração dos dados...")
    logging.info(f"Shape do DataFrame: {df.shape}")
    logging.info("Tipos de dados e valores não nulos:")
    df.info()
    logging.info("Resumo estatístico dos dados:")
    print(df.describe(include='all'))
    logging.info("Verificação de valores ausentes:")
    print(df.isnull().sum())
    logging.info("Amostra dos dados:")
    print(df.head())

def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Etapa 3: Limpeza
    Limpa os dados, renomeando colunas e removendo duplicatas.
    """
    logging.info("Iniciando limpeza dos dados...")
    
    # Renomeia colunas para maior clareza
    df_renamed = df.rename(columns={'id': 'id_municipio', 'nome': 'nome_municipio'})
    
    # Verifica e remove duplicatas baseadas no id do município
    if df_renamed.duplicated(subset=['id_municipio']).sum() > 0:
        df_cleaned = df_renamed.drop_duplicates(subset=['id_municipio'], keep='first')
        logging.info(f"Removidas {len(df_renamed) - len(df_cleaned)} linhas duplicadas.")
    else:
        df_cleaned = df_renamed
        logging.info("Nenhuma duplicata encontrada.")
        
    return df_cleaned

def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Etapa 4: Transformação
    Transforma os dados, extraindo e normalizando informações aninhadas
    """
    logging.info("Iniciando transformação dos dados...")

    df_normalized = pd.json_normalize(df['microrregiao'])

    # Extrai as colunas de interesse da estrutura achatada
    df['id_uf'] = df_normalized['mesorregiao.UF.id']
    df['sigla_uf'] = df_normalized['mesorregiao.UF.sigla']
    df['nome_uf'] = df_normalized['mesorregiao.UF.nome']

    # Remove a coluna original aninhada
    df_transformed = df.drop(columns=['microrregiao'])

    # Remove coluna que não me interessa
    df_transformed = df_transformed.drop(columns=['regiao-imediata'])

    # Garante que os tipos de dados estejam corretos após a extração
    df_transformed['id_municipio'] = pd.to_numeric(df_transformed['id_municipio'], errors='coerce')
    df_transformed['id_uf'] = pd.to_numeric(df_transformed['id_uf'], errors='coerce')
    
    logging.info("Dados transformados e colunas derivadas criadas.")
    return df_transformed

def enriquecer_dados(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Etapa 5: Enriquecimento
    Cruza os dados dos municípios com um CSV local para adicionar a macrorregião.
    """
    logging.info("Iniciando enriquecimento dos dados...")
    try:
        df_regioes = pd.read_csv(ENRICHMENT_DATA_PATH, encoding='utf-8')
        logging.info(f"Dados de enriquecimento carregados de '{ENRICHMENT_DATA_PATH}'.")
        
        # Realiza o merge (join) entre o DataFrame principal e o de regiões
        df_enriched = pd.merge(
            df, 
            df_regioes, 
            left_on='id_uf', 
            right_on='codigo_uf', 
            how='left'
        )
        
        # Remove a coluna de chave redundante do arquivo de enriquecimento
        df_enriched = df_enriched.drop(columns=['codigo_uf'])
        
        logging.info("Dados enriquecidos com sucesso com as informações da macrorregião.")
        return df_enriched
        
    except FileNotFoundError:
        logging.error(f"Arquivo de enriquecimento não encontrado em '{ENRICHMENT_DATA_PATH}'.")
        return None
    
def carregar_dados(df: pd.DataFrame):
    """
    Etapa 6: Carga (Load)
    Carrega os dados tratados e enriquecidos em um banco de dados MariaDB.
    """
    logging.info("Iniciando carga dos dados para o MariaDB...")
    
    # Coleta credenciais do banco de dados a partir das variáveis de ambiente
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    
    if not all([db_user, db_password, db_host, db_name]):
        logging.error("Credenciais do banco de dados não configuradas no arquivo .env. Abortando carga.")
        return

    try:
        # Cria a string de conexão e o engine do SQLAlchemy
        connection_string = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}"
        engine = create_engine(connection_string)

        # Define explicitamente os tipos de dados para as colunas da tabela SQL
        # Isso garante a criação correta da tabela (DDL)
        sql_types = {
            'id_municipio': types.BigInteger,
            'nome_municipio': types.VARCHAR(100),
            'id_uf': types.Integer,
            'sigla_uf': types.CHAR(2),
            'nome_uf': types.VARCHAR(50),
            'nome_regiao': types.VARCHAR(20)
        }
        
        # Carrega os dados para a tabela. `if_exists='replace'` recria a tabela a cada execução.
        # Isso torna o script idempotente (o resultado final é o mesmo, não importa quantas vezes seja executado).
        df.to_sql(
            DB_TABLE_NAME,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=sql_types
        )
        logging.info(f"Dados carregados com sucesso na tabela '{DB_TABLE_NAME}'.")

        # Adiciona a chave primária após a criação da tabela para garantir unicidade
        with engine.connect() as connection:
            connection.execute(text(f"ALTER TABLE {DB_TABLE_NAME} ADD PRIMARY KEY (id_municipio);"))
            logging.info(f"Chave primária adicionada à coluna 'id_municipio' da tabela '{DB_TABLE_NAME}'.")

    except Exception as e:
        logging.error(f"Erro ao carregar dados para o MariaDB: {e}")


def pipeline_completa():
    """
    Orquestrador principal que executa todas as etapas da pipeline de ETL.
    """
    logging.info("--- INICIANDO PIPELINE DE DADOS IBGE ---")
    
    df_raw = extrair_dados()
    if df_raw is None:
        logging.error("Pipeline interrompida na etapa de extração.")
        return
        
    explorar_dados(df_raw)
    df_cleaned = limpar_dados(df_raw)
    df_transformed = transformar_dados(df_cleaned)
    df_enriched = enriquecer_dados(df_transformed)
    
    if df_enriched is None:
        logging.error("Pipeline interrompida na etapa de enriquecimento.")
        return

    # Seleciona e reordena as colunas finais antes de carregar no banco
    colunas_finais = [
        'id_municipio', 'nome_municipio', 'id_uf', 'sigla_uf', 'nome_uf', 'nome_regiao'
    ]
    df_final = df_enriched[colunas_finais]
    
    logging.info("Visualização do DataFrame final antes da carga:")
    print(df_final.head())
    
    carregar_dados(df_final)
    
    logging.info("--- PIPELINE DE DADOS IBGE CONCLUÍDA COM SUCESSO ---")

if __name__ == "__main__":
    pipeline_completa()