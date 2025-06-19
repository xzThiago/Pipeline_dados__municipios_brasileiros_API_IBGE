# Projeto de Pipeline de Dados com API do IBGE

## Visão Geral

Este projeto demonstra a construção de uma pipeline de dados completa e realista, simulando o trabalho de um Engenheiro de Dados. O processo extrai dados públicos de municípios brasileiros da API do IBGE, passa por um ciclo completo de tratamento e os carrega em um banco de dados relacional (MariaDB) local.

O objetivo é criar um processo robusto, automatizado e idempotente, onde a execução repetida do script garante sempre o mesmo estado final consistente no banco de dados.

## Fluxo da Pipeline (ETL)

A pipeline é executada em uma sequência única e lógica, onde cada etapa prepara os dados para a seguinte:

**Extração → Exploração → Limpeza → Transformação → Enriquecimento → Carga**

## Tecnologias Utilizadas

- **Linguagem**: Python 3.9+
- **Banco de Dados**: MariaDB (compatível com MySQL)
- **Fonte de Dados**: API de Localidades do IBGE
- **Bibliotecas Principais**:
  - `requests`: Para realizar chamadas HTTP à API.
  - `pandas`: Para manipulação e transformação dos dados em memória.
  - `SQLAlchemy`: Para criar a conexão com o banco de dados de forma agnóstica.
  - `mysql-connector-python`: Driver para conectar ao MariaDB/MySQL.
  - `python-dotenv`: Para gerenciar variáveis de ambiente (credenciais) de forma segura.

## Estrutura do Projeto

Para executar, seu diretório deve conter os seguintes arquivos:
```
projeto-etl-ibge/
├── .env # Arquivo de credenciais do banco de dados (NÃO versionar)
├── projeto_etl_ibge.py # Script principal da pipeline
├── regioes_enriquecimento.csv # Arquivo CSV para enriquecimento dos dados
└── README.md # Este arquivo
```

## Como Configurar e Executar

Siga os passos abaixo para rodar a pipeline em seu ambiente local.

### 1. Pré-requisitos

- Python 3.9 ou superior instalado.
- Servidor MariaDB ou MySQL instalado e em execução.
- Git (opcional, para clonar o repositório).

### 2. Instalação

**Clone este repositório (ou crie os arquivos manualmente):**

```bash
git clone https://github.com/xzThiago/Pipeline_dados__municipios_brasileiros_API_IBGE.git
cd projeto-etl-ibge
```
* Instale as dependências Python: *
pip install requests pandas sqlalchemy mysql-connector-python python-dotenv

## Configuração do Ambiente
- Crie o banco de dados:

Conecte-se ao seu servidor MariaDB/MySQL e execute o seguinte comando:

*CREATE DATABASE IF NOT EXISTS engenharia_dados CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;*

- Crie o arquivo de credenciais (.env):

Crie um arquivo chamado .env na raiz do projeto e preencha com suas credenciais. Nunca suba este arquivo para um repositório Git.

# .env
```
DB_USER="seu_usuario_do_banco"
DB_PASSWORD="sua_senha_do_banco"
DB_HOST="localhost"
DB_NAME="engenharia_dados"
```

*Crie o arquivo de enriquecimento (regioes_enriquecimento.csv):*

Certifique-se de que o arquivo regioes_enriquecimento.csv existe no diretório com o conteúdo correto, mapeando o código da UF à sua respectiva região.

## Execução
Com tudo configurado, execute o script principal da pipeline:

python projeto_etl_ibge.py
