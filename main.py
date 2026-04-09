from src.config.messages import MSG_START_PIPELINE, MSG_PIPELINE_COMPLETE
from src.config.settings import PROCESSED_DATA_PATH
from src.database.loader import DataLoader
from src.handlers.transformer import DataTransformer
from src.services.exporter import DataExporter

def main():
    """
    Maestro do Pipeline ETL Financeiro.
    Orquestra o carregamento, transformação e exportação dos dados.
    """
    print(MSG_START_PIPELINE)

    # 1. Extração (Extract)
    loader = DataLoader()
    raw_data = loader.load_all_raw()

    if not raw_data:
        print("⚠️ Nenhum dado encontrado para processar. Finalizando.")
        return

    # 2. Transformação (Transform)
    transformer = DataTransformer()
    processed_data = transformer.transform_all(raw_data)

    # 3. Carga (Load / Export)
    exporter = DataExporter()
    exporter.export_to_csv(processed_data)

    # Opcional: Salvar no DB consolidado se configurado
    if 'consolidado' in processed_data:
        loader.save_to_db(processed_data['consolidado'], "f_consolidado_financeiro")

    print(MSG_PIPELINE_COMPLETE.format(path=PROCESSED_DATA_PATH))

if __name__ == "__main__":
    main()
