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
