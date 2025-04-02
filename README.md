## **Overview**

This project implements a robust ETL (Extract, Transform, Load) pipeline using **Dagster**, a modern data orchestrator. The pipeline processes PDF files, extracts text, generates structured data using OpenAI's API, and stores the results in a PostgreSQL database (either locally or on AWS RDS). The project is designed with scalability, modularity, and best practices in mind, making it suitable for both local development and cloud deployments.

---

## **Features**

- **PDF Text Extraction**: Reads PDF files from local storage or S3 and extracts text using the `unstructured` library.
- **Structured Data Generation**: Processes extracted text with OpenAI to produce structured JSON data.
- **PostgreSQL Storage**: Stores structured data in a PostgreSQL database for querying and analysis.
- **Dagster Integration**: Leverages Dagster's software-defined assets (SDAs) for modular pipeline orchestration.
- **Cloud-Ready**: Supports AWS RDS for PostgreSQL and S3 for storage.
- **Extensible Design**: Easily add new steps or modify existing ones without disrupting the pipeline.

---

## **Pipeline Workflow**

The pipeline consists of three sequential steps:

1. **PDF Text Extraction**:
    - Reads PDF files from a configurable storage backend (local filesystem or S3).
    - Extracts text using the `unstructured` library.
    - Saves the extracted text as JSON files.
2. **Structured Data Generation**:
    - Processes the extracted text with OpenAI's API.
    - Generates structured data based on a predefined schema.
    - Saves the structured data as JSON files.
3. **PostgreSQL Storage**:
    - Ingests the structured JSON files into a PostgreSQL database.
    - Creates tables dynamically based on the schema if they do not exist.

---

## **Project Structure**

```
dagster-pdf-pipeline/
├── README.md
├── pyproject.toml                # Project dependencies and configuration
├── src/
│   ├── __init__.py
│   ├── definitions.py            # Dagster pipeline definitions
│   ├── assets/
│   │   ├── _01_pdf_extraction.py # Step 1: Extract text from PDFs
│   │   ├── _02_openai_processing.py # Step 2: Generate structured data
│   │   └── _03_postgres_storage.py # Step 3: Store data in PostgreSQL
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── storage.py            # Handles local/S3 storage
│   │   ├── openai.py             # OpenAI API resource
│   │   └── postgres.py           # PostgreSQL resource
│   └── types/
│       ├── __init__.py
│       └── documents.py          # Data models for extracted/structured documents
├── tests/                        # Unit tests for assets/resources
├── Dockerfile                    # Docker container setup
├── docker-compose.yml            # Local development setup
└── terraform/                    # Infrastructure as code for AWS deployment
    ├── main.tf                   # AWS RDS and S3 configuration
    ├── variables.tf              # Terraform variables
    └── outputs.tf                # Terraform outputs
```

---

## **Setup Instructions**

### Prerequisites

1. Python 3.12+ installed.
2. PostgreSQL installed locally or an AWS RDS instance configured.
3. AWS CLI configured (if using S3 or RDS).
4. Docker installed (optional for containerized deployments).

### Installation

1. Clone the repository:

```bash
git clone https://github.com/your-repo/dagster-pdf-pipeline.git
cd dagster-pdf-pipeline
```

2. Install dependencies using `uv`:

```bash
uv pip install dagster dagster-aws dagster-postgres dagster-webserver unstructured openai psycopg2-binary boto3 pydantic pytest ruff pyright -e .
```

3. Set up environment variables:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=documents
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=yourpassword
export STORAGE_TYPE=local  # Use "s3" for AWS S3 storage
export LOCAL_STORAGE_PATH=./data  # Path to local storage directory
export OPENAI_API_KEY=your-openai-api-key
```

4. Initialize the database:

```bash
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "CREATE DATABASE documents;"
```


---

## **Running the Pipeline**

### Local Development

1. Start Dagster's development server:

```bash
dagster dev -m src.definitions --env .env.local
```

2. Open your browser and navigate to `http://localhost:3000` to access Dagster's UI.
3. Materialize assets by selecting them in the Dagster UI.

### Docker Deployment

1. Build the Docker image:

```bash
docker build -t dagster-pdf-pipeline .
```

2. Run the container:

```bash
docker run --env-file .env.local -p 3000:3000 dagster-pdf-pipeline
```


### Cloud Deployment with AWS

1. Configure Terraform variables in `terraform/variables.tf` (e.g., RDS instance details, S3 bucket name).
2. Deploy infrastructure:

```bash
terraform init &amp;&amp; terraform apply -auto-approve
```

3. Update environment variables with RDS/S3 details and deploy the pipeline.

---

## **Configuration**

### Environment Variables

| Variable | Description | Default |
| :-- | :-- | :-- |
| `POSTGRES_HOST` | PostgreSQL host | `localhost` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_DB` | Database name | `documents` |
| `POSTGRES_USER` | Database user | `postgres` |
| `POSTGRES_PASSWORD` | Database password | `yourpassword` |
| `STORAGE_TYPE` | Storage type (`local` or `s3`) | `local` |
| `LOCAL_STORAGE_PATH` | Local path for storing files | `./data` |
| `S3_BUCKET_NAME` | S3 bucket name | N/A |
| `OPENAI_API_KEY` | OpenAI API key | N/A |

---

## **Extending the Pipeline**

1. Add new steps by creating additional asset modules under `src/assets/`.
2. Define new resources in `src/resources/`.
3. Update pipeline definitions in `src/definitions.py`.

---

## **Testing**

Run unit tests using Pytest:

```bash
pytest tests/
```

---

## **License**

This project is licensed under [MIT License](LICENSE).

---

This README provides a detailed overview of your project, including setup instructions, pipeline workflow, and extensibility options. Let me know if you need further refinements!

<div>⁂</div>

[^1]: https://docs.dagster.io/getting-started/quickstart

[^2]: https://dev.to/alexserviceml/developing-in-dagster-2flh

[^3]: https://docs.dagster.io/guides/build/projects/structuring-your-dagster-project

[^4]: https://pypi.org/project/dagster/

[^5]: https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/README.md

[^6]: https://github.com/dagster-io/awesome-dagster/blob/main/README.md

[^7]: https://dagster.io

[^8]: https://dagster.io/blog/python-packages-primer-1
