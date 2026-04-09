import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configurações de Banco de Dados
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "projeto_financeiro")

# URLs de Conexão
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Caminhos de Dados
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")

# Lista de arquivos esperados
EXPECTED_FILES = [
    "cidade.csv",
    "estado.csv",
    "pais.csv",
    "contas.csv",
    "mov_pix.csv",
    "entrada_fluxo.csv",
    "saida_fluxo.csv"
]
