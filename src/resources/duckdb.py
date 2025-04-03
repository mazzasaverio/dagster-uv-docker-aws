from dagster import ConfigurableResource, get_dagster_logger
import duckdb
import os
from typing import Any, Dict, List, Optional, Union

logger = get_dagster_logger()


class DuckDBResource(ConfigurableResource):
    """Resource for DuckDB database operations using a file-based database."""

    path: Optional[str] = None
    read_only: bool = False

    def setup_for_execution(self, context) -> None:
        """Initialize DuckDB connection using environment variable or provided path."""
        db_path = self.path or os.environ.get("DUCKDB_PATH")
        if not db_path:
            raise ValueError(
                "DuckDB path must be provided either in config or DUCKDB_PATH environment variable"
            )

        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to database
        self._conn = duckdb.connect(database=db_path, read_only=self.read_only)
        logger.info(
            f"Connected to DuckDB at {db_path} ({'read-only' if self.read_only else 'read-write'})"
        )

    def create_table(self, table_name: str, schema: Dict[str, str]) -> None:
        """Create a table if it doesn't exist."""
        create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(f'{k} {v}' for k, v in schema.items())}
            );
        """
        self._conn.execute(create_stmt)
        logger.info(f"Created table {table_name} (if not exists)")

    def execute_query(self, query: str, params: Union[tuple, list, dict] = None) -> Any:
        """Execute a SQL query with optional parameters."""
        result = self._conn.execute(query, params if params else [])
        return result

    def execute_and_fetch(
        self, query: str, params: Union[tuple, list, dict] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as dictionaries."""
        result = self.execute_query(query, params)
        if result.description:
            columns = [desc[0] for desc in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]
        return []

    def execute_batch(self, query: str, data: List[tuple]) -> int:
        """Execute a batch operation with multiple parameter sets."""
        affected_rows = 0
        for params in data:
            result = self._conn.execute(query, params)
            affected_rows += result.execute_n_rows()
        return affected_rows

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        result = self._conn.execute(
            f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
        )
        return result.fetchone()[0] > 0

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    def close(self) -> None:
        """Close the database connection."""
        if hasattr(self, "_conn"):
            self._conn.close()
            logger.info("Closed DuckDB connection")
