# Delta Lake: Formato Aberto Transacional para Lakehouse

## 1. O que é o Delta Lake?

O Delta Lake é uma camada de armazenamento que adiciona recursos transacionais a Data Lakes. Ele utiliza arquivos Parquet para armazenar os dados e mantém um Transaction Log, localizado no diretório `_delta_log`, para registrar as alterações realizadas na tabela.

Entre os principais recursos disponibilizados pelo Delta Lake estão:

- Transações ACID;
- Controle de versões;
- Histórico das operações;
- Time Travel;
- Schema Enforcement;
- Schema Evolution;
- Operações de INSERT, UPDATE, DELETE e MERGE;
- Otimização das tabelas por meio do OPTIMIZE.

Estrutura simplificada:

Data Lake
|
+-- Arquivos de dados Parquet
|
+-- Tabela Delta
    |
    +-- Arquivos Parquet
    |
    +-- _delta_log
        |
        +-- Histórico das transações
        +-- Estado da tabela

---

## 2. O Delta Log (`_delta_log/`)

O diretório `_delta_log` é responsável pelo registro das alterações realizadas na tabela Delta.

As operações são registradas em arquivos JSON numerados sequencialmente, por exemplo:

    00000000000000000000.json
    00000000000000000001.json
    00000000000000000002.json

Também podem existir checkpoints em Parquet, utilizados para facilitar a reconstrução do estado da tabela.

No experimento, o Delta Log foi analisado diretamente para observar uma operação de UPDATE:

    log_v5 = spark.read.json(
        os.path.join(delta_log_path, "00000000000000000005.json")
    )

    log_v5.select(
        "commitInfo.operation",
        "commitInfo.operationParameters"
    ).show(truncate=False)

Resultado:

    |UPDATE|[{"(game_id#5555L = 8589946391)"}]|

Isso demonstra que a operação de atualização ficou registrada no Delta Log juntamente com sua condição.

---

## 3. Configuração do PySpark para Delta Lake

O ambiente utilizado no trabalho utiliza:

- Python;
- PySpark 3.5.3;
- Delta Lake 3.3.3;
- JupyterLab;
- Poetry como gerenciador do projeto.

A configuração utilizada no notebook foi:

    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("Trabalho Engenharia de Dados - Delta Lake")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

A versão do Spark utilizada no experimento foi 3.5.3 e o pacote Delta Lake utilizado foi o 3.3.3.

---

## 4. Dataset utilizado

Foi utilizado um dataset contendo informações sobre jogos avaliados pelo Metacritic.

Campos principais:

- name: string
- platform: string
- release_date: date
- metascore: double
- user_score: double
- developer: string
- publisher: string
- genre: string

Durante o tratamento inicial:

    Registros originais: 22.224
    Registros após remoção de duplicados: 21.913
    Duplicados removidos: 311

Foi utilizada a operação:

    df = df.dropDuplicates()

---

## 5. Criação da tabela Delta

Os dados foram armazenados em:

    data/delta/metacritic_games

Caminho utilizado:

    import os

    delta_path = os.path.abspath(
        "../../data/delta/metacritic_games"
    )

Gravação:

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_path)

Registro da tabela:

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS metacritic_games
        USING DELTA
        LOCATION '{delta_path}'
    """)

A coluna `game_id` foi utilizada como identificador técnico dos registros durante as demonstrações.

---

## 6. Operações de dados

Neste trabalho foram demonstradas as operações INSERT, UPDATE, DELETE e MERGE.

### 6.1 INSERT

    INSERT INTO metacritic_games (
        name,
        platform,
        release_date,
        metascore,
        user_score,
        developer,
        publisher,
        genre,
        game_id
    )
    SELECT
        name,
        platform,
        release_date,
        metascore,
        user_score,
        developer,
        publisher,
        genre,
        (SELECT MAX(game_id) + 1 FROM metacritic_games)
    FROM novo_jogo;

### 6.2 UPDATE

    UPDATE metacritic_games
    SET user_score = 9.0
    WHERE game_id = 8589946391;

O `user_score` passou de 8.5 para 9.0.

### 6.3 DELETE

    DELETE FROM metacritic_games
    WHERE game_id = 8589946391;

A consulta posterior confirmou que o registro não estava mais presente na versão atual.

### 6.4 MERGE INTO

    MERGE INTO metacritic_games AS target
    USING merge_data_atualizado AS source
    ON target.game_id = source.game_id

    WHEN MATCHED THEN
        UPDATE SET
            target.metascore = source.metascore,
            target.user_score = source.user_score

No experimento:

    game_id: 9999999999
    metascore: 95.0
    user_score: 9.7

---

## 7. Schema Enforcement

O Schema Enforcement impede que dados incompatíveis com o schema da tabela sejam inseridos.

Foi criado um DataFrame em que `release_date` possuía tipo string, enquanto a tabela esperava date.

    schema_invalido = spark.createDataFrame([
        (
            "Schema Test Game",
            "PC",
            "texto_invalido",
            80.0,
            8.0,
            "Test Studio",
            "Test Publisher",
            "Action",
            888888888
        )
    ], [
        "name",
        "platform",
        "release_date",
        "metascore",
        "user_score",
        "developer",
        "publisher",
        "genre",
        "game_id"
    ])

Ao realizar o append, a operação foi bloqueada:

    AnalysisException:
    [DELTA_FAILED_TO_MERGE_FIELDS]
    Failed to merge fields 'release_date' and 'release_date'

---

## 8. Schema Evolution

O Schema Evolution permite expandir o schema da tabela de forma controlada.

No experimento foi adicionada a coluna:

    rating_category

O schema passou a conter:

    |-- name: string
    |-- platform: string
    |-- release_date: date
    |-- metascore: double
    |-- user_score: double
    |-- developer: string
    |-- publisher: string
    |-- genre: string
    |-- game_id: long
    |-- rating_category: string

Consulta utilizada:

    SELECT
        name,
        metascore,
        user_score,
        game_id,
        rating_category
    FROM metacritic_games
    WHERE game_id = 7777777777;

Resultado:

    |Schema Evolution Game|88.0|8.8|7777777777|Excelente|

---

## 9. Time Travel

O Time Travel permite consultar versões anteriores de uma tabela Delta.

Foi possível consultar uma versão anterior mesmo após a exclusão do registro:

    SELECT *
    FROM metacritic_games VERSION AS OF 5
    WHERE game_id = 8589946391;

Resultado:

    name: Delta Lake Demo Game
    platform: PC
    metascore: 85.0
    user_score: 9.0
    game_id: 8589946391

Isso demonstra que o histórico transacional permite consultar o estado da tabela em uma versão anterior.

---

## 10. Histórico das operações

O histórico pode ser consultado utilizando:

    DESCRIBE HISTORY metacritic_games;

Durante o experimento foram registradas operações como:

    WRITE
    UPDATE
    DELETE
    OPTIMIZE

A operação de otimização foi registrada como:

    version: 11
    operation: OPTIMIZE

---

## 11. OPTIMIZE

O comando OPTIMIZE pode realizar compactação dos arquivos de dados.

Código utilizado:

    from delta.tables import DeltaTable

    delta_table = DeltaTable.forName(
        spark,
        "metacritic_games"
    )

    resultado_optimize = (
        delta_table
        .optimize()
        .executeCompaction()
    )

    resultado_optimize.show(truncate=False)

Antes:

    numFiles: 3
    sizeInBytes: aproximadamente 786 KB

Depois:

    numFiles: 1
    sizeInBytes: aproximadamente 665 KB

Nesse experimento foi utilizada somente a compactação. Não foi utilizado Z-Ordering.

---

## 12. Outros recursos de manutenção

### Z-Ordering

O Z-Ordering pode ser utilizado durante a otimização para organizar os dados com base em determinadas colunas.

Exemplo:

    OPTIMIZE metacritic_games
    ZORDER BY (platform);

Esse recurso não foi utilizado no experimento principal.

### VACUUM

O VACUUM é utilizado para remover arquivos de dados antigos que não são mais necessários pela tabela.

Essa operação não foi executada neste trabalho, pois os arquivos antigos são necessários para demonstrar o funcionamento do Time Travel.

---

## 13. Delta Lake vs. Apache Iceberg

Delta Lake e Apache Iceberg são formatos de tabela voltados para ambientes de Data Lake e Lakehouse.

| Critério | Apache Iceberg | Delta Lake |
|---|---|---|
| Formato de tabela | Iceberg | Delta |
| Arquivos de dados | Parquet e outros formatos suportados | Parquet |
| Transações | Sim | Sim |
| Schema Evolution | Sim | Sim |
| Time Travel | Sim | Sim |
| Histórico de alterações | Sim | Sim |
| Integração com Apache Spark | Sim | Sim |
| INSERT, UPDATE, DELETE | Sim | Sim |
| MERGE | Sim | Sim |
| Otimização | Recursos próprios do ecossistema | OPTIMIZE |
| Transaction Log | Metadados e snapshots do Iceberg | `_delta_log` |

Os dois formatos possuem objetivos semelhantes, mas utilizam mecanismos internos diferentes para controlar metadados, versões e alterações das tabelas.

---

## 14. Conclusão

A implementação demonstrou os principais recursos do Delta Lake utilizando PySpark.

Foram realizados experimentos com:

- criação de uma tabela Delta;
- inserção de registros;
- atualização de registros;
- exclusão de registros;
- MERGE;
- Schema Enforcement;
- Schema Evolution;
- Time Travel;
- análise do Delta Log;
- consulta do histórico;
- OPTIMIZE.

Os experimentos permitiram observar como o Delta Lake adiciona controle transacional, histórico e gerenciamento de versões a uma estrutura baseada em arquivos.

A implementação completa pode ser consultada no notebook:

    src/spark_delta_lake/delta-pyspark.ipynb
