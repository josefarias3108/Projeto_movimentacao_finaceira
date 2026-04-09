import pandas as pd
from src.config.messages import ERR_TRANSFORMATION

class DataTransformer:
    """
    Classe responsável pelas regras de negócio e transformação dos dados.
    """

    @staticmethod
    def process_dates(df, columns):
        """Converte colunas para datetime e cria formatos customizados"""
        try:
            for col in columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
                    # Formato amigável para relatórios
                    fmt_col = f"{col}_fmt"
                    df[fmt_col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            return df
        except Exception as e:
            print(ERR_TRANSFORMATION.format(error=f"Erro em datas: {str(e)}"))
            return df

    def transform_all(self, data):
        """Executa todas as transformações nos dataframes carregados"""
        # Processar mov_pix
        if 'mov_pix' in data:
            data['mov_pix'] = self.process_dates(
                data['mov_pix'], 
                ['pix_requested_at', 'pix_completed_at']
            )

        # Processar entrada_fluxo
        if 'entrada_fluxo' in data:
            data['entrada_fluxo'] = self.process_dates(
                data['entrada_fluxo'], 
                ['transaction_requested_at', 'transaction_completed_at']
            )

        # Processar saida_fluxo
        if 'saida_fluxo' in data:
            data['saida_fluxo'] = self.process_dates(
                data['saida_fluxo'], 
                ['transaction_requested_at', 'transaction_completed_at']
            )

        # Realizar Cruzamentos (Merge)
        if 'entrada_fluxo' in data and 'saida_fluxo' in data:
            data['consolidado'] = pd.merge(
                data['entrada_fluxo'], 
                data['saida_fluxo'], 
                on='account_id', 
                how='left',
                suffixes=('_entrada', '_saida')
            )
            
        return data
