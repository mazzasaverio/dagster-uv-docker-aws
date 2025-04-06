## **Overview**

Template to implement a pipeline using Dagster to extract text from PDF files, generate structured data using OpenAI's API, and store the results in a duckDB. The project is designed with scalability, modularity, and best practices in mind, making it suitable for both local development and cloud deployments.

- Quindi primo step


---

## **Features**

- **PDF Text Extraction**: Reads PDF files from local storage or S3 and extracts text using the `unstructured` library.
- **Structured Data Generation**: Processes extracted text with OpenAI to produce structured JSON data.
- **DuckDB Storage**: Store structured data in DuckDB file-based database
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

## **Setup Instructions**

### Prerequisites

1. Python 3.12+ installed.
3. AWS CLI configured (if using S3 or RDS).
4. Docker installed (optional for containerized deployments).



dagster dev -m src.definitions



Per visualizzare la ui con duckdb 

Prima installiamo 

curl https://install.duckdb.org | sh

duckdb data/documents.duckdb -ui   