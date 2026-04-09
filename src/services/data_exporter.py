import os
import csv
import psycopg
from dotenv import load_dotenv

def export_tables_to_csv():
    # Carrega variáveis de ambiente
    load_dotenv()
    
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_pass = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    raw_path = os.getenv('RAW_DATA_PATH', 'data/raw')
    
    if not os.path.exists(raw_path):
        os.makedirs(raw_path)

    tables = [
        'd_cidade', 'd_estado', 'd_pais', 'd_clientes', 
        'd_contas', 'f_entrada_fluxo', 'f_saida_fluxo', 'f_mov_pix'
    ]

    try:
        # Conexão usando psycopg3
        conn_info = f"dbname={db_name} user={db_user} password={db_pass} host={db_host} port={db_port}"
        with psycopg.connect(conn_info) as conn:
            with conn.cursor() as cur:
                print(f"Iniciando exportacao de {len(tables)} tabelas...")
                
                for table in tables:
                    file_path = os.path.join(raw_path, f"{table}.csv")
                    print(f"Exportando {table} para {file_path}...")
                    
                    cur.execute(f"SELECT * FROM {table}")
                    
                    colnames = [desc[0] for desc in cur.description]
                    
                    with open(file_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(colnames)
                        while True:
                            rows = cur.fetchmany(10000)
                            if not rows:
                                break
                            writer.writerows(rows)
                    
                    print(f"Tabela {table} exportada com sucesso.")

        print("\nTodas as tabelas foram trazidas para data/raw/ com sucesso!")
        
    except Exception as e:
        print(f"Erro na exportacao: {str(e)}")

if __name__ == "__main__":
    export_tables_to_csv()
