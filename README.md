# Trabalho Prático: Apache Spark, Apache Iceberg & Delta Lake

Projeto desenvolvido para a disciplina de **Engenharia de Dados**, com o objetivo de demonstrar, na prática, o uso do **Apache Spark (PySpark)** e de formatos modernos de tabelas para arquiteturas **Data Lakehouse**, utilizando **Apache Iceberg** e **Delta Lake**.

O projeto apresenta fundamentação teórica, configuração do ambiente, processamento dos dados e experimentos práticos com operações de manipulação, gerenciamento de schema, versionamento e otimização das tabelas.

---

## 📚 Documentação

A documentação completa do projeto foi desenvolvida utilizando **MkDocs** e **Material for MkDocs**.

Ela apresenta:

- contextualização do projeto;
- conceitos fundamentais do Apache Spark e PySpark;
- conceitos e implementação prática do Apache Iceberg;
- conceitos e implementação prática do Delta Lake;
- modelo Entidade-Relacionamento;
- estrutura das tabelas;
- comandos DDL;
- operações `INSERT`, `UPDATE`, `DELETE` e `MERGE`;
- Schema Enforcement;
- Schema Evolution;
- Time Travel;
- histórico de operações;
- Delta Log;
- otimização das tabelas.

### Visualizar localmente

Após instalar as dependências do projeto:

```bash
poetry install
```

Execute:

```bash
poetry run mkdocs serve
```

A documentação estará disponível em:

```text
http://127.0.0.1:8000/
```

### Documentação publicada

[> Link do GitHub Pages (`mkdocs gh-deploy`).](https://jeanvitorvieira.github.io/trabalho-eng-dados-apache-spark)

---

## 🛠️ Tecnologias utilizadas

- **Python**
- **Apache Spark**
- **PySpark 3.5.3**
- **Delta Lake 3.3.3**
- **Apache Iceberg**
- **JupyterLab**
- **Poetry**
- **MkDocs**
- **Material for MkDocs**
- **Pandas**
- **Parquet**
- **Git/GitHub**

---

## 📋 Pré-requisitos

Para executar o projeto, é necessário possuir:

- Python compatível com a versão definida no `pyproject.toml`;
- Java/JDK compatível com o Apache Spark;
- Poetry;
- Git.

As dependências Python utilizadas pelo projeto são gerenciadas pelo Poetry através do arquivo:

```text
pyproject.toml
```

---

## 🚀 Instalação

Clone o repositório:

```bash
git clone https://github.com/jeanvitorvieira/trabalho-eng-dados-apache-spark.git
```

Entre no diretório:

```bash
cd trabalho-eng-dados-apache-spark
```

Instale as dependências:

```bash
poetry install
```

Para executar comandos utilizando o ambiente virtual do projeto:

```bash
poetry run <comando>
```

---

## 📓 Execução dos notebooks

Os experimentos práticos estão organizados em notebooks Jupyter.

### Apache Iceberg

```text
src/spark_iceberg/iceberg-pyspark.ipynb
```

### Delta Lake

```text
src/spark_delta_lake/delta-pyspark.ipynb
```

Para iniciar o JupyterLab:

```bash
poetry run jupyter lab
```

Após iniciar o JupyterLab, abra o notebook desejado e execute as células em sequência.

---

## 🗂️ Estrutura do repositório

```text
trabalho-eng-dados-apache-spark/
│
├── data/
│   ├── metacritic_dataset_raw_sep_2025.csv
│   └── metacritic_dataset_clean_sep_2025.csv
│
├── docs/
│   ├── index.md
│   ├── spark.md
│   ├── iceberg.md
│   └── delta.md
│
├── src/
│   ├── spark_iceberg/
│   │   ├── __init__.py
│   │   └── iceberg-pyspark.ipynb
│   │
│   └── spark_delta_lake/
│       ├── __init__.py
│       └── delta-pyspark.ipynb
│
├── tests/
│   └── __init__.py
│
├── .gitignore
├── .python-version
├── mkdocs.yml
├── poetry.lock
├── pyproject.toml
├── README.md
└── LICENSE
```

> O diretório `data/delta/` é utilizado para o armazenamento local da tabela Delta durante os experimentos e não deve ser versionado no Git.

---

## 📊 Dataset

O projeto utiliza um dataset relacionado a jogos avaliados pelo **Metacritic**.

Os arquivos disponibilizados no projeto são:

```text
data/metacritic_dataset_raw_sep_2025.csv
data/metacritic_dataset_clean_sep_2025.csv
```

No laboratório do Delta Lake, o dataset possui inicialmente **22.224 registros**.

Após a aplicação de `dropDuplicates()`, foram obtidos **21.913 registros**, com a remoção de **311 registros duplicados**.

O dataset tratado é utilizado como base para a criação das tabelas e para os experimentos realizados nos notebooks.

---

## 🧪 Experimentos realizados

### Apache Spark / PySpark

A documentação apresenta os principais conceitos relacionados ao Apache Spark e ao PySpark, incluindo:

- processamento distribuído;
- SparkSession;
- DataFrames;
- SQL;
- execução de operações;
- integração com formatos de tabela.

### Apache Iceberg

O laboratório do Iceberg aborda a criação e manipulação de tabelas utilizando Apache Spark.

São apresentados experimentos envolvendo:

- criação de tabelas;
- DDL;
- ingestão;
- operações de dados;
- gerenciamento das tabelas;
- demais recursos demonstrados no notebook.

### Delta Lake

O laboratório do Delta Lake aborda:

- criação de tabela Delta;
- `INSERT`;
- `UPDATE`;
- `DELETE`;
- `MERGE`;
- Schema Enforcement;
- Schema Evolution;
- Time Travel;
- histórico de operações;
- análise do `_delta_log`;
- `OPTIMIZE`.

---

## 👥 Divisão de responsabilidades

O trabalho foi desenvolvido em equipe, com divisão das atividades entre os integrantes.

### Mateus

Responsável por:

- documentação do **Apache Spark (PySpark)** em `docs/spark.md`;
- documentação do **Apache Iceberg** em `docs/iceberg.md`;
- implementação prática do laboratório Iceberg em:

```text
src/spark_iceberg/iceberg-pyspark.ipynb
```

### Jean Vitor Vieira

Responsável por:

- documentação do **Delta Lake** em `docs/delta.md`;
- implementação prática do laboratório Delta Lake em:

```text
src/spark_delta_lake/delta-pyspark.ipynb
```

- experimentos adicionais relacionados ao Delta Lake;
- integração das evidências práticas na documentação;
- revisão dos requisitos de documentação e reprodução relacionados ao módulo.

### Trabalho em conjunto

As etapas de integração do projeto, organização do repositório, documentação geral, configuração do MkDocs e preparação para apresentação foram realizadas em conjunto pela equipe.

---

## 📖 Documentação do projeto

A documentação está organizada nas seguintes páginas:

| Página | Conteúdo |
|---|---|
| `index.md` | Contextualização e visão geral |
| `spark.md` | Apache Spark e PySpark |
| `iceberg.md` | Apache Iceberg |
| `delta.md` | Delta Lake |

---

## 🔬 Objetivo dos experimentos

O objetivo do projeto não é apenas apresentar os conceitos dos formatos, mas demonstrar seu funcionamento através de experimentos executáveis.

Dessa forma, os notebooks apresentam tanto a implementação quanto os resultados obtidos durante a execução das operações.

---

## 📚 Referências

As referências utilizadas na fundamentação teórica e nos experimentos estão disponíveis na documentação do projeto e nos notebooks correspondentes.

Entre as principais fontes utilizadas estão as documentações oficiais de:

- Apache Spark;
- Apache Iceberg;
- Delta Lake;
- PySpark;
- MkDocs.

---

## 👨‍💻 Autores

**Jean Vitor Vieira**

**Mateus**

Trabalho acadêmico desenvolvido para a disciplina de **Engenharia de Dados**.
