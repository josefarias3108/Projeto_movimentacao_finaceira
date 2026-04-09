import os
from src.config.settings import PROCESSED_DATA_PATH
from src.config.messages import MSG_EXPORT_SUCCESS

class DataExporter:
    """
    Classe responsável por salvar os dados processados.
    """

    def export_to_csv(self, data_dict):
        """Salva todos os dataframes processados em CSVs"""
        if not os.path.exists(PROCESSED_DATA_PATH):
            os.makedirs(PROCESSED_DATA_PATH)

        for name, df in data_dict.items():
            path = os.path.join(PROCESSED_DATA_PATH, f"{name}_processed.csv")
            df.to_csv(path, index=False)
        
        print(MSG_EXPORT_SUCCESS)
        return True
