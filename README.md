# Trabalho Prático: Apache Spark, Apache Iceberg & Delta Lake

Projeto desenvolvido para a disciplina de Engenharia de Dados, demonstrando na prática arquiteturas de **Data Lakehouse** através do processamento distribuído com **Apache Spark (PySpark)** e formatos modernos de tabela aberta (**Apache Iceberg** e **Delta Lake**).

---

## 📚 Documentação Web Interativa (MkDocs)

O projeto conta com uma documentação web completa e estilizada com **Material for MkDocs**, contendo diagramas de arquitetura, modelo Entidade-Relacionamento, códigos DDL, fundamentação teórica e evidências práticas de comandos `INSERT`, `UPDATE` e `DELETE`.

### Como Visualizar a Documentação Localmente

1. Certifique-se de que as dependências do Poetry estão instaladas:
   ```bash
   poetry install
   ```

2. Inicie o servidor de desenvolvimento do MkDocs:
   ```bash
   poetry run mkdocs serve
   ```

3. Acesse a documentação no navegador através do endereço:
   👉 **`http://127.0.0.1:8000/`**

---

## 🗂 Estrutura do Repositório

```
spark-iceberg/
├── data/                                         # Conjunto de dados (Metacritic Games)
│   ├── metacritic_dataset_raw_sep_2025.csv       # Dados brutos (36.831 linhas)
│   └── metacritic_dataset_clean_sep_2025.csv     # Dados saneados de referência
├── docs/                                         # Conteúdo Markdown da documentação MkDocs
│   ├── index.md                                  # Contextualização e Visão Geral
│   ├── spark.md                                  # Explicação profunda do Apache Spark & PySpark
│   ├── iceberg.md                                # Explicação e Evidências Práticas do Apache Iceberg
│   └── delta.md                                  # Módulo atribuído ao colega (Delta Lake)
├── src/
│   ├── spark_iceberg/                            # Laboratório prático do Apache Iceberg
│   │   └── iceberg-pyspark.ipynb                 # Notebook com DDL, Ingestão, Quarentena e CRUD
│   └── spark_delta_lake/                         # Laboratório prático do Delta Lake
├── spark-warehouse/                              # Armazenamento local das tabelas Iceberg (bronze)
├── mkdocs.yml                                    # Configurações do site e tema Material
└── pyproject.toml                                # Dependências e ambiente do projeto Poetry
```

---

## 👥 Divisão de Módulos da Equipe

- **Mateus**:
  - Página de documentação do **Apache Spark (PySpark)** (`docs/spark.md`)
  - Página de documentação do **Apache Iceberg** (`docs/iceberg.md`)
  - Implementação prática do laboratório Iceberg no Jupyter Notebook (`src/spark_iceberg/iceberg-pyspark.ipynb`)
- **Colega de Equipe**:
  - Contextualização detalhada do trabalho (`docs/index.md`)
  - Página de documentação do **Delta Lake** (`docs/delta.md`)
  - Implementação prática do laboratório Delta Lake no Jupyter Notebook (`src/spark_delta_lake/`)
