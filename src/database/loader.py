import os
import pandas as pd
from sqlalchemy import create_engine
from src.config.settings import DATABASE_URL, RAW_DATA_PATH, EXPECTED_FILES
from src.config.messages import MSG_LOAD_SUCCESS, ERR_FILE_NOT_FOUND, ERR_DB_CONNECTION

class DataLoader:
    """
    Classe responsável por carregar os dados brutos de arquivos CSV 
    ou diretamente do banco de dados PostgreSQL.
    """
    
    def __init__(self):
        self.engine = None
        try:
            self.engine = create_engine(DATABASE_URL)
        except Exception as e:
            print(ERR_DB_CONNECTION.format(error=str(e)))

    def load_csv(self, filename):
        """Carrega um arquivo CSV da pasta data/raw"""
        path = os.path.join(RAW_DATA_PATH, filename)
        if not os.path.exists(path):
            print(ERR_FILE_NOT_FOUND.format(filename=filename))
            return None
        
        df = pd.read_csv(path, sep=",")
        print(MSG_LOAD_SUCCESS.format(filename=filename))
        return df

    def load_all_raw(self):
        """Carrega todos os arquivos CSV esperados em um dicionário"""
        dataframes = {}
        for file in EXPECTED_FILES:
            df = self.load_csv(file)
            if df is not None:
                name = file.replace(".csv", "")
                dataframes[name] = df
        return dataframes

    def save_to_db(self, df, table_name):
        """Salva um DataFrame no PostgreSQL"""
        if self.engine:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                return True
            except Exception as e:
                print(f"Erro ao salvar no banco: {str(e)}")
        return False

    def load_from_db(self, table_name):
        """Carrega uma tabela do PostgreSQL para um DataFrame"""
        if self.engine:
            try:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, self.engine)
                print(f"✅ Tabela {table_name} carregada do banco de dados.")
                return df
            except Exception as e:
                print(f"❌ Erro ao carregar tabela {table_name}: {str(e)}")
        return None

    def load_all_data(self):
        """Tenta carregar de CSVs primeiro, se falhar, tenta do Banco de Dados"""
        data = self.load_all_raw()
        
        # Se não encontrou CSVs, tenta carregar do DB
        if not data and self.engine:
            print("🔄 CSVs não encontrados. Tentando carregar do Banco de Dados...")
            tables = [
                'd_cidade', 'd_estado', 'd_pais', 'd_clientes', 
                'd_contas', 'f_entrada_fluxo', 'f_saida_fluxo', 'f_mov_pix'
            ]
            for t in tables:
                df = self.load_from_db(t)
                if df is not None:
                    # Remove o prefixo d_ ou f_ para manter consistência com o notebook
                    name = t.split('_', 1)[1] if '_' in t else t
                    data[name] = df
        return data
