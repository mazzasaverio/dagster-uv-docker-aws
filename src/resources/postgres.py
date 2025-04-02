from dagster import ConfigurableResource, get_dagster_logger
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from typing import Any, Dict, List

logger = get_dagster_logger()


class PostgreSQLResource(ConfigurableResource):
    """Manages PostgreSQL/RDS connections and operations"""

    host: str
    port: int = 5432  # Dagster gestirà la conversione automaticamente
    dbname: str
    user: str
    password: str

    def validate_config(self) -> None:
        """Validate resource configuration"""
        if not all([self.host, self.dbname, self.user, self.password]):
            raise ValueError("All PostgreSQL connection parameters must be provided")

    def setup_for_execution(self, context) -> None:
        """Initialize database connection"""
        self.validate_config()
        self._conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
        self._conn.autocommit = False

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as dicts"""
        with self._conn.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return []

    def execute_batch(self, query: str, data: List[tuple]) -> int:
        """Execute a batch INSERT/UPDATE operation"""
        with self._conn.cursor() as cursor:
            execute_batch(cursor, query, data)
            rowcount = cursor.rowcount
            self._conn.commit()
        return rowcount

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        query = """
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        return self.execute_query(query, (table_name,))[0]["exists"]

    def create_documents_table(self, schema: Dict[str, str]) -> None:
        """Create table with dynamic schema"""
        if self.table_exists("documents"):
            return

        fields = [
            sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(data_type))
            for col_name, data_type in schema.items()
        ]

        query = sql.SQL(
            """
            CREATE TABLE documents (
                id SERIAL PRIMARY KEY,
                {fields},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        ).format(fields=sql.SQL(",\n").join(fields))

        with self._conn.cursor() as cursor:
            cursor.execute(query)
            self._conn.commit()
