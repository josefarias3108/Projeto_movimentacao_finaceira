# 💰 Projeto de Análise de Movimentação Financeira

Projeto profissional de Engenharia de Dados focado na análise de fluxos financeiros, conversão de dados transacionais e preparação para dashboards (BI).

## 🚀 Arquitetura do Projeto

O projeto segue uma arquitetura modular **Maestro/Handlers**, separando responsabilidades para garantir escalabilidade e fácil manutenção:

- **`main.py` (Maestro)**: Orquestra o fluxo ETL (Extração, Transformação e Carga).
- **`src/handlers/`**: Contém as regras de negócio e funções de limpeza de dados.
- **`src/database/`**: Lógica de conexão com banco de dados (PostgreSQL) e carregamento de arquivos.
- **`src/config/`**: Configurações globais, caminhos e mensagens de log.
- **`src/services/`**: Serviços auxiliares de exportação e reporte.

## 🛠️ Tecnologias Utilizadas

- **Python 3.12+**
- **Pandas**: Manipulação e análise de dados.
- **SQLAlchemy / Psycopg2**: Integração nativa com PostgreSQL.
- **Power BI**: (Arquivo incluído em `docs/`) para visualização de dados.

## 📋 Como Configurar

1. **Dependências**:
   Instale as bibliotecas necessárias:
   ```bash
   pip install -r requirements.txt
   ```

2. **Variáveis de Ambiente**:
   Crie um arquivo `.env` baseado no `.env.example` e adicione suas credenciais do Postgres:
   ```env
   DB_USER=seu_usuario
   DB_PASSWORD=sua_senha
   DB_HOST=localhost
   DB_NAME=projeto_financeiro
   ```

3. **Arquivos de Dados**:
   Coloque os arquivos CSV originais na pasta `data/raw/`.

4. **Execução**:
   Rode o pipeline principal:
   ```bash
   python main.py
   ```

## 📂 Organização de Pastas

```text
├── data/               # Dados brutos e processados
├── docs/               # Documentação técnica e Power BI
├── notebooks/          # Estudos originais e experimentação
├── src/                # Código fonte modularizado
├── main.py             # Script principal (Orquestrador)
└── requirements.txt    # Lista de dependências
```

---
Desenvolvido com foco em excelência técnica e limpeza de código.
