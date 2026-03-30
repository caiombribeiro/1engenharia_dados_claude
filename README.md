# Brazilian E-commerce ETL Pipeline

End-to-end ETL pipeline using the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

Raw CSV files are ingested into PostgreSQL, transformed with dbt, and orchestrated daily by Apache Airflow — all running locally with a single `docker compose up`.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│                                                                 │
│  ┌──────────────┐    ┌──────────────────────────────────────┐  │
│  │   data/      │    │           PostgreSQL 15               │  │
│  │  (CSVs)      │    │                                      │  │
│  │              │    │  ┌──────────┐ ┌─────────┐ ┌───────┐ │  │
│  └──────┬───────┘    │  │   raw    │ │ staging │ │ marts │ │  │
│         │            │  └────▲─────┘ └────▲────┘ └───▲───┘ │  │
│         │            │       │             │           │     │  │
│  ┌──────▼───────┐    │       │        ┌────┴───────────┘     │  │
│  │  ingestion/  ├────┼───────┘        │   dbt models        │  │
│  │  ingest.py   │    │                │   staging/ (views)  │  │
│  └──────────────┘    │                │   marts/  (tables)  │  │
│                      └────────────────┴─────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Apache Airflow 2.9  (LocalExecutor)         │  │
│  │                                                          │  │
│  │  check_files → ingest_raw → dbt_deps → dbt_staging       │  │
│  │                          → dbt_marts → dbt_test          │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python + pandas + SQLAlchemy |
| Storage | PostgreSQL 15 |
| Transformation | dbt-postgres 1.7 |
| Orchestration | Apache Airflow 2.9 |
| Infrastructure | Docker Compose |

---

## How Each Tool Works in This Project

### Docker & Docker Compose

Docker permite empacotar uma aplicação junto com todas as suas dependências em um **container** — um ambiente isolado que roda igual em qualquer máquina, independente do sistema operacional.

O `docker-compose.yml` define e sobe todos os serviços do projeto com um único comando:

| Service | O que faz |
|---|---|
| `postgres` | Banco de dados PostgreSQL com os schemas `raw`, `staging` e `marts` |
| `airflow-init` | Roda uma vez só: inicializa o banco do Airflow e cria o usuário admin |
| `airflow-webserver` | Serve a UI do Airflow em `http://localhost:8080` |
| `airflow-scheduler` | Monitora as DAGs e agenda/executa as tasks |

Todos os serviços do Airflow usam a mesma imagem customizada (definida no `Dockerfile`), que parte da imagem oficial do Airflow e instala o `dbt-postgres`, `pandas` e `psycopg2` em cima.

Os dados são compartilhados entre containers via **volumes** — por exemplo, a pasta `./dbt` do seu computador é montada como `/opt/dbt` dentro dos containers do Airflow, o que permite que o scheduler execute os modelos dbt sem precisar copiá-los para dentro da imagem.

---

### Apache Airflow

Airflow é uma plataforma de **orquestração de pipelines de dados**. Em vez de rodar scripts manualmente ou com cron jobs soltos, você define um pipeline como um **DAG** (Directed Acyclic Graph) — um conjunto de tasks com dependências entre elas.

Neste projeto, a DAG `olist_etl_pipeline` define 6 tasks em sequência:

```
check_files → ingest_raw → dbt_deps → dbt_staging → dbt_marts → dbt_test
```

| Task | Tipo | O que faz |
|---|---|---|
| `check_files` | PythonOperator | Verifica se todos os 9 CSVs estão presentes antes de começar |
| `ingest_raw` | PythonOperator | Carrega os CSVs para o schema `raw` do PostgreSQL |
| `dbt_deps` | BashOperator | Instala os pacotes dbt (ex: `dbt_utils`) |
| `dbt_staging` | BashOperator | Executa os 7 models de staging (`dbt run --select staging`) |
| `dbt_marts` | BashOperator | Executa os 3 models de marts (`dbt run --select marts`) |
| `dbt_test` | BashOperator | Roda os 32 testes de qualidade de dados (`dbt test`) |

Se qualquer task falhar, o Airflow para a execução e marca as tasks seguintes como bloqueadas — garantindo que dados ruins não avancem para o próximo estágio.

A DAG está configurada com `schedule_interval="@daily"`, ou seja, roda automaticamente uma vez por dia. Pode ser disparada manualmente pela UI sempre que necessário.

---

### dbt (data build tool)

dbt é uma ferramenta de **transformação de dados dentro do banco de dados**. Em vez de escrever scripts Python para transformar dados e salvar resultados, você escreve **SQL puro** e o dbt cuida de criar as views e tabelas no PostgreSQL na ordem certa.

O projeto segue a arquitetura em duas camadas:

#### Camada Staging — `dbt/models/staging/`

Views criadas sobre o schema `raw`. Cada model corresponde a uma tabela bruta e tem como único objetivo **limpar e padronizar** os dados:

- Converter colunas de texto para os tipos corretos (`::timestamp`, `::numeric`, `::int`)
- Renomear colunas para snake_case consistente
- Tratar valores nulos e strings vazias
- Normalizar campos de texto (ex: `upper(state)`, `initcap(city)`)

As views de staging **não duplicam dados** — elas são apenas uma camada de leitura sobre o `raw`.

```sql
-- Exemplo: stg_orders.sql
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp  as purchased_at,
    order_approved_at::timestamp         as approved_at,
    ...
from {{ source('raw', 'orders') }}
where order_id is not null
```

#### Camada Marts — `dbt/models/marts/`

Tabelas materializadas que respondem **perguntas de negócio**. São construídas em cima dos models de staging — nunca acessam o `raw` diretamente.

| Model | Pergunta respondida |
|---|---|
| `mart_revenue_by_state` | Quais estados geram mais receita? Qual o ticket médio por estado/ano? |
| `mart_monthly_orders` | Como o volume de pedidos evolui mês a mês? Quantos foram entregues vs cancelados? |
| `mart_avg_ticket_by_category` | Quais categorias de produto têm o maior ticket médio? Qual o peso do frete por categoria? |

#### Testes de qualidade de dados

O dbt executa **32 testes automaticamente** após cada run:

- **Testes genéricos** (definidos em `_schema.yml`): `not_null`, `unique`, `accepted_values`
- **Testes singulares** (em `tests/`): queries SQL customizadas que retornam zero linhas se o dado estiver correto

Se qualquer teste falhar, o Airflow marca a DAG como falha — impedindo que dados inconsistentes alimentem análises.

---

## Results

### Revenue by State
![Revenue by State](docs/mart_revenue_by_state.png)

São Paulo leads with R$ 3.3M in 2018, followed by Rio de Janeiro and Minas Gerais.

### Top Categories by Average Ticket
![Top Categories by Average Ticket](docs/top_categories_by_average_ticket.png)

`watches_gifts` has the highest average ticket (R$ 212), while `health_beauty` leads in order volume.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Compose v2)
- Kaggle account to download the dataset

---

## Quickstart

### 1. Clone the repository

```bash
git clone <repo-url>
cd brazilian-ecommerce-etl
```

### 2. Download the dataset

Download from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), unzip and move all CSV files into the `data/` folder:

```
data/
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_customers_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

### 3. Configure environment (Linux only)

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

On Windows and macOS with Docker Desktop this step can be skipped.

### 4. Build and start all services

```bash
docker compose up --build -d
```

This will:
1. Build the custom Airflow image with dbt installed
2. Start PostgreSQL and create the `olist` + `airflow` databases
3. Initialize Airflow (migrate DB, create admin user)
4. Start the Airflow webserver and scheduler

Wait ~2 minutes for initialization to complete. Monitor with:

```bash
docker compose ps        # all services should show "healthy" or "running"
docker compose logs -f airflow-init   # watch initialization progress
```

### 5. Trigger the pipeline

Open **http://localhost:8080** → login with `admin` / `admin`

1. Find the `olist_etl_pipeline` DAG
2. Toggle it **On**
3. Click **▶ Trigger DAG** to run immediately

Watch the tasks turn green one by one in the **Graph** view.

### 6. Query the results

```bash
docker compose exec postgres psql -U olist -d olist
```

```sql
-- Top 10 states by revenue
SELECT * FROM marts.mart_revenue_by_state ORDER BY total_revenue DESC LIMIT 10;

-- Monthly order trend
SELECT * FROM marts.mart_monthly_orders ORDER BY order_month;

-- Top categories by average ticket
SELECT * FROM marts.mart_avg_ticket_by_category ORDER BY avg_ticket DESC LIMIT 10;
```

---

## Project Structure

```
.
├── Dockerfile                  # Airflow 2.9 image + dbt-postgres + pandas
├── docker-compose.yml          # Services: postgres, airflow-init, webserver, scheduler
├── .env.example                # Environment variables template
│
├── data/                       # CSV files go here (gitignored)
├── postgres/
│   └── init_db.sh              # Creates olist + airflow databases on first run
│
├── ingestion/
│   └── ingest.py               # Loads all 9 CSVs into PostgreSQL raw schema
│
├── dbt/
│   ├── dbt_project.yml         # Project config: model paths, materializations per layer
│   ├── profiles.yml            # DB connection — reads credentials from env vars
│   ├── packages.yml            # dbt_utils dependency
│   ├── models/
│   │   ├── staging/            # 7 views: type casting + column renaming
│   │   └── marts/              # 3 tables: business metrics
│   └── tests/
│       └── assert_no_negative_prices.sql
│
└── airflow/
    └── dags/
        └── olist_etl_dag.py    # Pipeline DAG with 6 tasks
```

---

## Useful Commands

```bash
# Follow logs for a specific service
docker compose logs -f airflow-scheduler

# Run dbt manually (outside the DAG)
docker compose exec airflow-scheduler \
  dbt run --profiles-dir /opt/dbt --project-dir /opt/dbt

# Run only the tests
docker compose exec airflow-scheduler \
  dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt

# Run a specific model
docker compose exec airflow-scheduler \
  dbt run --profiles-dir /opt/dbt --project-dir /opt/dbt --select mart_revenue_by_state

# Tear down (keeps PostgreSQL data volume)
docker compose down

# Tear down and wipe all data
docker compose down -v
```
