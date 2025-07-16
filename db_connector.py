"""
Redshift Database Connector
Adapted from redshift_mcp_server.py for Streamlit use
"""
import os
import logging
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
import pandas as pd

# Try to import redshift_connector, fallback to psycopg2
try:
    import redshift_connector
    HAS_REDSHIFT_CONNECTOR = True
    logger = logging.getLogger(__name__)
    logger.info("Successfully imported redshift_connector")
except ImportError as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to import redshift_connector: {e}")
    try:
        import psycopg2
        HAS_REDSHIFT_CONNECTOR = False
        logger.info("Using psycopg2 as fallback")
    except ImportError:
        logger.error("Neither redshift_connector nor psycopg2 is available")
        raise ImportError("Neither redshift_connector nor psycopg2 is available. Please install one of them.")

# Configure logging
logging.basicConfig(level=logging.INFO)

@dataclass
class ConnectionState:
    """State management for database connection"""
    conn: Optional[Any] = None
    cursor: Optional[Any] = None
    host: Optional[str] = None
    database: Optional[str] = None
    user: Optional[str] = None
    is_connected: bool = False

class RedshiftConnector:
    """Redshift database connector with query safety"""
    
    def __init__(self):
        self.connection_state = ConnectionState()
        self.mode = os.getenv('DB_MCP_MODE', 'readonly').lower()
        
    def get_mcp_mode(self) -> str:
        """Get the current MCP mode"""
        if self.mode not in ('readonly', 'readwrite', 'admin'):
            return 'readonly'
        return self.mode
    
    def is_forbidden(self, sql: str) -> Optional[str]:
        """Check if SQL statement is forbidden in current mode"""
        FORBIDDEN_READONLY = [
            'insert', 'update', 'delete', 'drop', 'truncate', 'alter', 
            'create', 'grant', 'revoke', 'comment', 'set', 'copy', 
            'unload', 'vacuum', 'analyze', 'merge'
        ]
        
        sql_trim = sql.strip().lower()
        first_word = sql_trim.split()[0] if sql_trim else ''
        
        mode = self.get_mcp_mode()
        if mode == 'readonly':
            if first_word in FORBIDDEN_READONLY:
                return f"'{first_word.upper()}' statements are not allowed in readonly mode."
        
        return None
    
    def connect(self, host: str, database: str, user: str, 
                password: str, port: int = 5439) -> Tuple[bool, str]:
        """Connect to Redshift database"""
        try:
            # Close existing connection if any
            if self.connection_state.conn:
                self.connection_state.conn.close()
            
            # Create new connection using appropriate connector
            if HAS_REDSHIFT_CONNECTOR:
                # Use redshift_connector
                self.connection_state.conn = redshift_connector.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                logger.info("Using redshift_connector for connection")
            else:
                # Use psycopg2 as fallback
                self.connection_state.conn = psycopg2.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                logger.info("Using psycopg2 for connection")
            
            self.connection_state.cursor = self.connection_state.conn.cursor()
            self.connection_state.host = host
            self.connection_state.database = database
            self.connection_state.user = user
            self.connection_state.is_connected = True
            
            logger.info(f"Connected to {host}/{database}")
            return True, f"Successfully connected to {host}/{database}"
            
        except Exception as e:
            logger.error(f"Connection failed: {str(e)}")
            self.connection_state.is_connected = False
            return False, f"Connection failed: {str(e)}"
    
    def disconnect(self) -> Tuple[bool, str]:
        """Disconnect from database"""
        try:
            if self.connection_state.conn:
                self.connection_state.conn.close()
                self.connection_state = ConnectionState()
            return True, "Disconnected successfully"
        except Exception as e:
            logger.error(f"Disconnect failed: {str(e)}")
            return False, f"Disconnect failed: {str(e)}"
    
    def execute_query(self, sql: str, params: Optional[List[Any]] = None) -> Tuple[bool, Any]:
        logger.info(f"RedshiftConnector: Executing SQL: {sql}")
        if not self.connection_state.is_connected:
            return False, "Not connected to database"
        
        # Check if query is forbidden
        forbidden_reason = self.is_forbidden(sql)
        if forbidden_reason:
            return False, forbidden_reason
        
        try:
            cursor = self.connection_state.cursor
            
            # Execute query
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # Fetch results
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Convert to DataFrame
            if rows and columns:
                df = pd.DataFrame(rows, columns=columns)
                return True, df
            else:
                return True, pd.DataFrame()  # Empty DataFrame
                
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            # Auto-rollback to clear error state
            try:
                self.connection_state.conn.rollback()
            except Exception as rollback_err:
                logger.error(f"Rollback failed: {rollback_err}")
            return False, f"Query failed: {str(e)}"
    
    def list_schemas(self) -> Tuple[bool, Any]:
        """List all schemas in the database"""
        sql = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name
        """
        return self.execute_query(sql)
    
    def list_tables(self, schema: str = "public") -> Tuple[bool, Any]:
        """List tables in a specific schema"""
        sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        return self.execute_query(sql, [schema])
    
    def describe_table(self, table: str, schema: str = "public") -> Tuple[bool, Any]:
        """Get table structure"""
        sql = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.execute_query(sql, [schema, table])
    
    def get_all_schema_names(self) -> list:
        """Return a list of all user schema names (excluding system schemas)"""
        success, schemas_df = self.list_schemas()
        if not success or 'schema_name' not in schemas_df.columns:
            return []
        return [s for s in schemas_df['schema_name'].tolist() if s not in ('pg_catalog', 'information_schema')]

    def get_table_schema_info(self, schema: str = None, max_tables_per_schema: int = 5) -> str:
        """Get formatted schema information for LLM context. If schema is None or 'ALL', include all schemas."""
        schema_infos = []
        if not schema or schema.upper() == 'ALL':
            schema_names = self.get_all_schema_names()
        else:
            schema_names = [schema]
        for sch in schema_names:
            success, tables_df = self.list_tables(sch)
            if not success or 'table_name' not in tables_df.columns:
                continue
            schema_info = f"Schema: {sch}\nTables:\n"
            for table in tables_df['table_name'].tolist()[:max_tables_per_schema]:
                success, columns_df = self.describe_table(table, sch)
                if success:
                    schema_info += f"\n{table}:\n"
                    for _, col in columns_df.iterrows():
                        col_info = f"  - {col['column_name']} ({col['data_type']}"
                        if col['character_maximum_length']:
                            col_info += f"({col['character_maximum_length']})"
                        elif col['numeric_precision']:
                            col_info += f"({col['numeric_precision']}"
                            if col['numeric_scale']:
                                col_info += f",{col['numeric_scale']}"
                            col_info += ")"
                        col_info += f", nullable: {col['is_nullable']}"
                        if col['column_default']:
                            col_info += f", default: {col['column_default']}"
                        col_info += ")\n"
                        schema_info += col_info
            schema_infos.append(schema_info)
        return "\n\n".join(schema_infos) if schema_infos else "No schema info available."
    
    def is_connected(self) -> bool:
        """Check if connected to database"""
        return self.connection_state.is_connected
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get current connection information"""
        return {
            "host": self.connection_state.host,
            "database": self.connection_state.database,
            "user": self.connection_state.user,
            "is_connected": self.connection_state.is_connected,
            "mode": self.get_mcp_mode()
        } 

    def get_performance_system_schema_info(self) -> str:
        """Return schema info for key Redshift system tables/views for performance analysis."""
        # Define the system tables/views and their key columns
        system_tables = {
            'STL_QUERY': [
                'userid', 'query', 'starttime', 'endtime', 'elapsed', 'aborted', 'rows', 'label', 'text'
            ],
            'SVV_TABLE_INFO': [
                'table', 'schema', 'size', 'tbl_rows', 'unsorted', 'stats_off', 'pct_used'
            ],
            'SVV_DISKUSAGE': [
                'schema', 'table', 'size', 'perm_table_rows', 'spectrum_table_rows'
            ],
            'SVL_QLOG': [
                'userid', 'query', 'starttime', 'endtime', 'aborted', 'elapsed', 'label', 'text'
            ],
            'STL_ALERT_EVENT_LOG': [
                'event_time', 'event_severity', 'event_message', 'solution'
            ]
        }
        schema_info = "Amazon Redshift System Tables/Views for Performance Analysis:\n"
        for table, columns in system_tables.items():
            schema_info += f"\n{table}:\n"
            for col in columns:
                schema_info += f"  - {col}\n"
        return schema_info 