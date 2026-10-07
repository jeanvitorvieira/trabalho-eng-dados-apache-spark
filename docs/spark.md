# Apache Spark & PySpark: Processamento de Dados em Larga Escala

---

## 1. Introdução e Contexto Histórico

O **Apache Spark** é uma das tecnologias mais fundamentais e amplamente adotadas no cenário moderno de Engenharia de Dados e Computação Distribuída. Desenvolvido originalmente em 2009 no AMPLab da Universidade da Califórnia em Berkeley (liderado por Matei Zaharia) e posteriormente doado à Apache Software Foundation em 2013, o Spark nasceu com o propósito explícito de superar os graves gargalos de desempenho e usabilidade do modelo **Hadoop MapReduce**.

```text
flowchart TD
    subgraph HadoopMapReduce["Hadoop MapReduce (Baseado em Disco)"]
        H1[Entrada HDFS] --> M1[Map 1]
        M1 --> D1[Disco HDFS]
        D1 --> R1[Reduce 1]
        R1 --> D2[Disco HDFS]
        D2 --> M2[Map 2 (Próxima Fase)]
        M2 --> D3[Disco HDFS]
        D3 --> R2[Reduce 2]
        R2 --> S1[Saída Final HDFS]
    end

    subgraph ApacheSpark["Apache Spark (Computação In-Memory)"]
        SIn[Entrada de Dados] --> T1[Transformação 1]
        T1 -->|Memória / Cache| T2[Transformação 2]
        T2 -->|Memória / Cache| T3[Transformação 3]
        T3 --> SOut[Ação / Saída Final]
    end
```

### O Gargalo do Hadoop MapReduce
No modelo tradicional do MapReduce, qualquer pipeline com múltiplos passos analíticos, algoritmos iterativos (como os de Machine Learning) ou consultas interativas exigia a gravação e leitura repetitiva dos resultados intermediários diretamente em disco rígido no sistema de arquivos distribuído (HDFS). Isso causava:

- **Intensa latência de I/O de disco**: Cada estágio `Map` e `Reduce` lia e persistia blocos no storage físico.
- **Overhead maciço de serialização/desserialização**: Os dados precisavam ser constantemente convertidos de e para formatos graváveis.
- **Complexidade de desenvolvimento**: Escrever código MapReduce em baixo nível exigia centenas de linhas de código Java "boilerplate".

### O Salto do Apache Spark
O Spark revolucionou essa abordagem ao introduzir o **processamento orientado à memória principal (RAM)**. Ao manter conjuntos de dados intermediários em memória através de grafos acíclicos dirigidos (**DAGs**), o Spark alcança velocidades até **100 vezes superiores** ao Hadoop MapReduce para algoritmos iterativos e até **10 vezes superiores** em operações de consulta baseadas em disco.

Além da velocidade, o Spark consolidou em uma única engine unificada o suporte para:
1. Processamento em Lote (*Batch Processing*).
2. Consultas Interativas SQL (*Spark SQL*).
3. Processamento de Fluxos Contínuos (*Structured Streaming*).
4. Aprendizado de Máquina Distribuído (*MLlib*).
5. Processamento de Grafos (*GraphX*).

---

## 2. O que é o PySpark e Como Funciona por Baixo dos Panos

O **PySpark** é a interface e biblioteca Python oficial para o Apache Spark. Ele permite que desenvolvedores e cientistas de dados aproveitem a simplicidade, flexibilidade e o rico ecossistema de bibliotecas do Python (como Pandas, NumPy e Scikit-Learn) combinados com o poder de computação distribuída massiva do Spark escrito na JVM (Java Virtual Machine / Scala).

### A Ponte Py4J e a Arquitetura de Execução do PySpark

Muitos desenvolvedores acreditam erroneamente que o PySpark executa todo o processamento em Python puro. Na realidade, o núcleo do Spark opera na JVM. O PySpark utiliza uma ponte de comunicação interprocessos (**IPC via Sockets TCP**) viabilizada pela biblioteca **Py4J**.

```text
flowchart TB
    subgraph DriverNode["Nó Driver"]
        PyDriver["Processo Python (Driver)<br>Código do Usuário / Script / Jupyter"]
        Py4J["Py4J Gateway (Socket IPC)"]
        JVMDriver["Processo JVM (Driver)<br>SparkSession / SparkContext<br>DAGScheduler / TaskScheduler"]

        PyDriver <-->|Comandos & Planos Lógicos| Py4J
        Py4J <-->|Chamadas de Métodos Java| JVMDriver
    end

    subgraph ClusterMgr["Cluster Manager (YARN / K8s / Standalone)"]
        CM["Alocação de Recursos e Containers"]
    end

    subgraph Worker1["Worker Node / Executor 1"]
        JVMExec1["JVM Executor<br>Task Threads / Off-Heap Memory<br>Catalyst & Tungsten"]
        PyWorker1["Python Daemon / Worker<br>(Apenas para UDFs / RDDs Python)"]
        
        JVMExec1 <-->|Pipes / Sockets / Apache Arrow| PyWorker1
    end

    subgraph Worker2["Worker Node / Executor 2"]
        JVMExec2["JVM Executor<br>Task Threads / Off-Heap Memory<br>Catalyst & Tungsten"]
        PyWorker2["Python Daemon / Worker<br>(Apenas para UDFs / RDDs Python)"]
        
        JVMExec2 <-->|Pipes / Sockets / Apache Arrow| PyWorker2
    end

    JVMDriver -->|Gerencia Recursos| CM
    CM -->|Lança Containers| JVMExec1
    CM -->|Lança Containers| JVMExec2
    JVMDriver -->|Envia Tasks e Partições| JVMExec1
    JVMDriver -->|Envia Tasks e Partições| JVMExec2
```

### Detalhamento da Execução:

1. **No Nó Driver**: Quando você executa um comando PySpark (como `df.filter(...)`), o processo Python não processa os dados diretamente. Ele traduz a instrução em chamadas de métodos Java/Scala na JVM do Driver via **Py4J**.
2. **Construção do Plano**: A JVM cria o plano lógico e físico da consulta através do motor **Catalyst Optimizer**.
3. **Distribuição para os Executors**: O Spark divide o trabalho em **Tasks** e as envia para os executores do cluster.
4. **Execução Nativa vs. Python Workers**:
   - **Operações com Spark SQL / DataFrames Nativos**: São executadas inteiramente dentro da JVM em código compilado de alta velocidade (Project Tungsten). Não há transferência de dados para processos Python nos executores!
   - **Operações com UDFs (User-Defined Functions) em Python**: Exigem que o executor da JVM crie um processo filho (`python daemon`). Os dados precisam ser serializados da memória JVM, transferidos via socket/pipe para o processo Python, processados, serializados de volta e devolvidos à JVM.
   - **Otimização com Apache Arrow**: Nas versões modernas do PySpark, o **Apache Arrow** é utilizado para transferir dados colunares entre a JVM e o Python com zero-copy e serialização de alto desempenho, reduzindo drasticamente o gargalo de UDFs vetorizadas (como `pandas_udf`).

---

## 3. Arquitetura Distribuída do Apache Spark

O Apache Spark opera sob o modelo mestre-escravo (*Master-Slave* ou *Driver-Worker*):

```text
classDiagram
    class DriverProgram {
        +SparkSession
        +SparkContext
        +DAGScheduler
        +TaskScheduler
        +Coordena a execução
        +Coleta resultados
    }
    class ClusterManager {
        +Aloca nós e recursos
        +YARN / K8s / Standalone
    }
    class WorkerNode {
        +Executors
        +Cores / CPU Slots
        +Memória RAM / Cache
    }
    class Executor {
        +Executa Tasks
        +Armazena blocos de dados
    }

    DriverProgram --> ClusterManager : Solicita recursos
    ClusterManager --> WorkerNode : Inicializa
    WorkerNode *-- Executor : Contém
    DriverProgram --> Executor : Envia Tasks diretamente
```

### Componentes Chave da Arquitetura:

1. **Driver Program**:
   - O "cérebro" da aplicação Spark.
   - Contém a `SparkSession` e a função principal do programa.
   - Converte o programa do usuário em um grafo de dependências (**DAG - Directed Acyclic Graph**).
   - Divide o DAG em **Jobs**, **Stages** e **Tasks**.
   - O **DAGScheduler** divide os stages nas fronteiras de shuffle; o **TaskScheduler** submete as tarefas aos executores no cluster.

2. **Cluster Manager**:
   - Componente plugável responsável por alocar os recursos físicos de hardware do cluster.
   - O Spark suporta:
     - **Standalone**: Gerenciador nativo e simples do próprio Spark.
     - **Apache YARN**: Padrão comum em clusters corporativos Hadoop.
     - **Kubernetes (K8s)**: O padrão moderno para infraestruturas nativas em nuvem (*cloud-native*), permitindo orquestração em contêineres e auto-escalonamento.
     - **Apache Mesos**: Suporte legado para centros de processamento heterogêneos.

3. **Worker Nodes & Executors**:
   - Nós de trabalho que executam os processos **Executors**.
   - Cada **Executor** é um processo JVM individual dedicado à aplicação, possuindo uma quantidade configurável de núcleos de CPU (*cores*) e memória RAM.
   - Os executores realizam duas tarefas essenciais:
     1. Executam as **Tasks** computacionais designadas pelo Driver.
     2. Armazenam dados em cache e memória (*Block Manager*) para reutilização entre estágios.

---

## 4. Abstrações Fundamentais: RDD, DataFrame e Dataset

O Apache Spark evoluiu suas estruturas de dados ao longo dos anos para equilibrar flexibilidade de programação com otimização automática de hardware.

```text
timeline
    title Evolução das Abstrações do Apache Spark
    2011 : RDD (Resilient Distributed Datasets) : Baixo nível, flexível, controle total, sem otimização de schema
    2015 : DataFrames : Estruturado, baseado em colunas, Catalyst Optimizer, tipagem dinâmica
    2016 : Datasets : Fortemente tipado (compile-time safety), unificação com DataFrames na JVM
```

### 1. RDD (Resilient Distributed Dataset)
O RDD é a abstração básica e primordial do Spark:
- **Resilient (Resiliente)**: Tolerante a falhas. Se um nó falha, o Spark reconstrói a partição perdida recalculando o grafo de linhagem (*lineage graph*), sem necessidade de replicar dados fisicamente em disco.
- **Distributed (Distribuído)**: Os dados são fragmentados em partições espalhadas por múltiplos nós do cluster.
- **Dataset (Conjunto de Dados)**: Contém coleções de objetos Java/Python brutos e imutáveis.

### 2. DataFrames
Introduzido com o Spark SQL, o DataFrame é uma coleção distribuída organizada em colunas nomeadas, conceitualmente semelhante a uma tabela em um banco de dados relacional ou a um `pandas.DataFrame`, porém com o motor distribuído do Spark por baixo:
- Possui um **Schema** explícito (nomes e tipos de dados como `StringType`, `IntegerType`, `DateType`).
- Permite que o Spark aplique o **Catalyst Optimizer** e o **Project Tungsten**, tornando as consultas ordens de grandeza mais rápidas que operações puras em RDDs.

### 3. Datasets
Disponível em linguagens de tipagem estática (Scala e Java), o Dataset combina a segurança de tipagem em tempo de compilação (*compile-time type safety*) com as otimizações do Catalyst. Em Python (PySpark), como a linguagem é dinamicamente tipada, o `DataFrame` é a interface primária e unificada.

### Comparativo Técnico entre as Abstrações

| Característica | RDD | DataFrame | Dataset (JVM) |
| :--- | :--- | :--- | :--- |
| **Nível de Abstração** | Baixo nível (orientado a objetos) | Alto nível (estruturado / relacional) | Alto nível fortemente tipado |
| **Otimização de Consulta** | Nenhuma (cabe ao desenvolvedor) | **Catalyst Optimizer & Tungsten** | **Catalyst Optimizer & Tungsten** |
| **Type Safety** | Tempo de execução (ou compilação em Scala) | Tempo de execução (Runtime) | Tempo de compilação (Compile-time) |
| **Formato na Memória** | Objetos Java serializados na JVM | Formato colunar binário compacto (Off-Heap) | Formato colunar binário com encoders |
| **Linguagens Recomendadas** | Casos muito específicos de dados não estruturados | **Python (PySpark)**, SQL, Scala, R | Scala, Java |
| **Facilidade de Uso** | Requer `map`, `reduceByKey`, `flatMap` | Suporte fluente a SQL ANSI, `.select()`, `.filter()` | Expressões de tipo fortemente vinculadas |

---

## 5. Modelo de Execução: Lazy Evaluation, DAG e Shuffle

Compreender o ciclo de vida de uma aplicação Spark é essencial para arquitetar pipelines de alto rendimento.

### Avaliação Preguiçosa (*Lazy Evaluation*)

No Apache Spark, as operações aplicadas a um DataFrame dividem-se rigorosamente em duas categorias:

```text
flowchart LR
    subgraph Transformacoes["Transformações (Lazy / Não executam imediatamente)"]
        T1["df.filter(...)"]
        T2["df.select(...)"]
        T3["df.withColumn(...)"]
        T4["df.groupBy(...)"]
    end

    subgraph Acoes["Ações (Trigger / Disparam a execução)"]
        A1["df.show()"]
        A2["df.count()"]
        A3["df.collect()"]
        A4["df.write.save()"]
    end

    Transformacoes -->|Cria o Grafo Lógico (DAG)| Acoes
```

- **Transformações**: Criam um novo DataFrame a partir de um existente (ex.: `filter()`, `select()`, `groupBy()`, `join()`). O Spark **não processa nenhum dado** quando uma transformação é declarada; ele apenas anota a operação no grafo de linhagem.
- **Ações**: Instruem o Spark a computar o resultado final e devolvê-lo ao Driver ou persistí-lo no storage (ex.: `show()`, `count()`, `collect()`, `writeTo().append()`). Somente quando uma ação é chamada, o grafo é compilado e executado.

> **Por que isso é revolucionário?**
> A avaliação preguiçosa permite que o motor de otimização visualize a consulta inteira antes de tocá-la no disco. Se você fizer 5 filtros e 10 seleções seguidas, o Spark reordena e funde essas operações em um único predicado unificado, evitando leitura e transporte inútil de dados.

---

### Narrow vs. Wide Transformations e o Custo do Shuffle

A distinção mais crítica de desempenho no Spark reside em como os dados se movem entre as partições do cluster:

```text
flowchart TD
    subgraph Narrow["Transformações Estreitas (Narrow Transformations) - SEM SHUFFLE"]
        direction LR
        P1A[Partição 1] --> P1B[Partição 1 Resultante]
        P2A[Partição 2] --> P2B[Partição 2 Resultante]
        P3A[Partição 3] --> P3B[Partição 3 Resultante]
    end

    subgraph Wide["Transformações Largas (Wide Transformations) - COM SHUFFLE DE REDE"]
        direction LR
        WP1[Partição 1] -.-> RP1[Partição A]
        WP1 -.-> RP2[Partição B]
        WP2[Partição 2] -.-> RP1
        WP2 -.-> RP2
        WP3[Partição 3] -.-> RP1
        WP3 -.-> RP2
    end
```

1. **Transformações Estreitas (Narrow Transformations)**:
   - Cada partição de entrada é consumida por **no máximo uma** partição de saída.
   - Não há necessidade de comunicação ou troca de dados entre nós pela rede.
   - O processamento ocorre localmente e em paralelo absoluto.
   - **Exemplos**: `filter()`, `select()`, `map()`, `withColumn()`, `drop()`.

2. **Transformações Largas (Wide Transformations)**:
   - Dados de múltiplas partições precisam ser redistribuídos e agrupados em novas partições baseando-se em uma chave (ex.: `platform` ou `genre`).
   - Esse processo de redistribuição chama-se **SHUFFLE**.
   - O Shuffle exige gravação temporária de dados em disco no nó emissor, transferência intensa através da rede local do cluster e leitura no nó receptor. É a operação mais custosa em processamento distribuído.
   - **Exemplos**: `groupBy()`, `join()`, `distinct()`, `orderBy()`, `repartition()`.

---

### Hierarquia de Execução: Jobs, Stages e Tasks

Quando uma ação é disparada, o Spark decompõe o trabalho na seguinte hierarquia:

```text
flowchart TD
    App[Aplicação Spark] --> J1[Job 1 (Disparado pela Ação df.count)]
    App --> J2[Job 2 (Disparado pela Ação df.writeTo.append)]
    
    J2 --> S1[Stage 1 (Narrow: Leitura e Filtros)]
    J2 -->|SHUFFLE BOUNDARY| S2[Stage 2 (Wide: Agrupamento / Escrita)]
    
    S1 --> T1A[Task 1 (Partição 1)]
    S1 --> T1B[Task 2 (Partição 2)]
    S1 --> T1C[Task 3 (Partição 3)]
    
    S2 --> T2A[Task 4]
    S2 --> T2B[Task 5]
```

- **Job**: Corresponde a uma ação invocada no código (ex.: cada `.count()` ou `.write` gera um Job).
- **Stage**: Cada Job é dividido em estágios. A fronteira de um Stage é sempre delimitada por uma operação que exige **Shuffle**. Enquanto houver apenas Narrow Transformations, o Spark encadeia tudo no mesmo Stage (*Pipeline Execution*).
- **Task**: A unidade elementar e atômica de trabalho. Uma task é enviada para rodar em uma única partição de dados dentro de uma única thread de um Executor.

---

## 6. Motores de Otimização Internos: Catalyst e Tungsten

O diferencial que torna o Apache Spark o padrão industrial de mercado é a combinação de dois motores internos de alta tecnologia: o **Catalyst Optimizer** e o **Project Tungsten**.

### 1. Catalyst Optimizer (Otimizador Baseado em Árvore)

O Catalyst converte a expressão relacional do usuário no plano físico mais eficiente possível através de 4 fases estruturadas:

```text
flowchart LR
    Code["Código SQL / DataFrame"] --> P1["1. Análise<br>(Resolve nomes no Catálogo)"]
    P1 --> P2["2. Otimização Lógica<br>(Regras de reescrita / Predicate Pushdown)"]
    P2 --> P3["3. Planejamento Físico<br>(Estratégias de Join / Hash vs Sort)"]
    P3 --> P4["4. Geração de Código<br>(Whole-Stage CodeGen / Java Bytecode)"]
    P4 --> Exec["Execução nos Nós"]
```

- **Fase 1 - Análise**: O Catalyst valida se as tabelas, colunas e tipos existem consultando o catálogo de metadados. Transforma a AST (*Abstract Syntax Tree*) em um plano lógico resolvido.
- **Fase 2 - Otimização de Plano Lógico**: Aplica dezenas de regras matemáticas e relacionais padrão:
  - *Predicate Pushdown*: Empurra os filtros `WHERE` o mais próximo possível da fonte de dados (ex.: leitura de Parquet ou Iceberg), evitando ler dados que serão descartados depois.
  - *Projection Pruning*: Lê apenas as colunas necessárias, ignorando as demais.
  - *Constant Folding*: Pré-calcula expressões estáticas (ex.: converte `1 + 1` diretamente em `2` em tempo de planejamento).
- **Fase 3 - Planejamento Físico**: Avalia diferentes planos físicos possíveis (ex.: se um `join` deve ser feito via *Broadcast Hash Join* ou *Sort-Merge Join*) e escolhe o de menor custo estimado.
- **Fase 4 - Whole-Stage Code Generation**: Compila o plano físico inteiro em bytecode Java puro em tempo de execução, fundindo múltiplos operadores em uma única função Java otimizada, eliminando chamadas virtuais de função e maximizando a localidade de registradores de CPU.

### 2. Project Tungsten (Eficiência de Hardware em Baixo Nível)

O Tungsten foca na eficiência de hardware moderna (CPU e memória):
- **Gerenciamento de Memória Off-Heap**: O Tungsten aloca blocos de memória brutos fora do gerenciamento do Garbage Collector (GC) da JVM usando a API `sun.misc.Unsafe`. Isso elimina por completo as pausas catastróficas de coleta de lixo (*GC Pauses*) em clusters com centenas de gigabytes de RAM.
- **Estruturas Binárias Compactas**: Armazena dados em formatos binários proprietários alinhados aos registradores de memória da CPU, evitando o overhead de encapsulamento de objetos Java.
- **Computação Consciente de Cache (Cache-Aware Computation)**: Projeta algoritmos de ordenação e junção que mantêm os dados críticos armazenados diretamente nas memórias cache L1, L2 e L3 da CPU, minimizando acessos à memória RAM principal.

---

## 7. Inicialização Prática e Melhores Práticas com PySpark

### Configuração da SparkSession para Lakehouse

Para que o PySpark se conecte a formatos avançados de Data Lakehouse como **Apache Iceberg**, é necessário estender a sessão com os pacotes Maven e catálogos correspondentes:

```python
from pyspark.sql import SparkSession

# Inicialização de uma SparkSession preparada para Apache Iceberg
spark = (
    SparkSession.builder
    .appName("PySpark-Lakehouse-Engine")
    # Pacote com o runtime do Iceberg para Spark 3.5 e Scala 2.12
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1")
    # Extensões SQL do Iceberg (suporte a UPDATE, DELETE, MERGE, CALL)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Configuração do Catálogo local baseado no sistema de arquivos (Hadoop Catalog)
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg")
    # Otimização de partições padrão para operações de shuffle
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```

---

### Diretrizes de Desempenho e Boas Práticas

1. **Evite Python UDFs Tradicionais**:
   - Prefira sempre as funções nativas de `pyspark.sql.functions` (como `F.when()`, `F.col()`, `F.regexp_replace()`). Elas executam dentro do Catalyst/Tungsten na JVM sem custo de IPC.
   - Caso seja estritamente necessário usar Python customizado, utilize **Pandas UDFs** (`@pandas_udf`) vetorizadas com Apache Arrow.

2. **Utilize Broadcast Joins em Tabelas Pequenas**:
   - Ao unir uma tabela grande (ex.: Fato de Vendas) com uma tabela dimensional pequena (ex.: Tabela de Plataformas), use `F.broadcast(dimensao_df)`. Isso envia a tabela pequena inteira para cada executor, eliminando o Shuffle de rede da tabela grande.

3. **Gerenciamento do "Small Files Problem"**:
   - Um número excessivo de partições gera milhares de arquivos Parquet minúsculos no Data Lake, degradando o tempo de listagem e leitura.
   - Utilize `.coalesce(n)` para diminuir partições sem shuffle antes de persistir dados, ou configure compactação automática no formato de tabela.

4. **Persistência Consciente com `cache()`**:
   - Utilize `.cache()` ou `.persist(StorageLevel.MEMORY_AND_DISK)` apenas quando um mesmo DataFrame for reutilizado em múltiplas ações subsequentes. Chamar cache sem necessidade desperdiça memória valiosa dos executores.

---

> Próximo passo: Compreenda como o **Apache Iceberg** complementa o Apache Spark provendo integridade transacional ACID, evolução de schema e Time Travel na página [Apache Iceberg](iceberg.md).
