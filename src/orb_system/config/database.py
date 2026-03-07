import mysql.connector
from mysql.connector import pooling
from contextlib import contextmanager
import os
from typing import Generator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig:
    """Database connection configuration for ORB System"""
    
    def __init__(self):
        # Default to orb_trading database
        db_url = os.getenv('DATABASE_URL', 'mysql+pymysql://root@localhost/bharatquant_sniper')
        # Extract components if needed, but for now we'll use specific env vars or defaults
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 3306)),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'orb_trading'),
            'pool_name': 'orb_pool',
            'pool_size': 10
        }
        
        # Create connection pool
        try:
            self.pool = mysql.connector.pooling.MySQLConnectionPool(**self.config)
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            raise e
    
    @contextmanager
    def get_connection(self) -> Generator:
        """Context manager for database connections"""
        conn = self.pool.get_connection()
        try:
            yield conn
            if conn.autocommit is False:
                conn.commit()
        except Exception as e:
            try:
                conn.rollback()
            except:
                pass
            raise e
        finally:
            conn.close()
    
    @contextmanager
    def get_cursor(self, dictionary=True) -> Generator:
        """Context manager for database cursor"""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=dictionary)
            try:
                yield cursor
            finally:
                cursor.close()

# Global database instance
db = DatabaseConfig()
