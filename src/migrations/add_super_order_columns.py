"""
Database Migration: Add Super Order Columns to orb_signals Table

Adds target_order_id and sl_order_id columns to track Dhan super order IDs.
"""
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Parse DATABASE_URL
db_url = os.getenv("DATABASE_URL")
# Format: mysql+pymysql://user:password@host:port/database
parts = db_url.replace("mysql+pymysql://", "").split("@")
user_pass = parts[0].split(":")
host_db = parts[1].split("/")
host_port = host_db[0].split(":")

config = {
    'user': user_pass[0],
    'password': user_pass[1],
    'host': host_port[0],
    'port': int(host_port[1]) if len(host_port) > 1 else 3306,
    'database': host_db[1]
}

print(f"Connecting to database: {config['database']} at {config['host']}:{config['port']}")

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("\\n=== Adding Super Order Columns to orb_signals ===\\n")
    
    # Check if columns already exist
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'orb_signals' 
        AND COLUMN_NAME IN ('target_order_id', 'sl_order_id')
    """, (config['database'],))
    
    existing_columns = [row[0] for row in cursor.fetchall()]
    
    if 'target_order_id' in existing_columns and 'sl_order_id' in existing_columns:
        print("✅ Columns already exist. No migration needed.")
    else:
        # Add target_order_id column
        if 'target_order_id' not in existing_columns:
            print("Adding column: target_order_id...")
            cursor.execute("""
                ALTER TABLE orb_signals 
                ADD COLUMN target_order_id VARCHAR(50) NULL 
                COMMENT 'Dhan order ID for target order (super order)'
            """)
            print("✅ Added target_order_id column")
        else:
            print("⏭️  target_order_id already exists")
        
        # Add sl_order_id column
        if 'sl_order_id' not in existing_columns:
            print("Adding column: sl_order_id...")
            cursor.execute("""
                ALTER TABLE orb_signals 
                ADD COLUMN sl_order_id VARCHAR(50) NULL 
                COMMENT 'Dhan order ID for stop-loss order (super order)'
            """)
            print("✅ Added sl_order_id column")
        else:
            print("⏭️  sl_order_id already exists")
        
        conn.commit()
        print("\\n✅ Migration completed successfully!")
    
    # Verify the schema
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_COMMENT 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'orb_signals' 
        AND COLUMN_NAME IN ('execution_pos_id', 'target_order_id', 'sl_order_id')
        ORDER BY ORDINAL_POSITION
    """, (config['database'],))
    
    print("\\n=== Order ID Columns in orb_signals ===")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} - {row[2]}")
    
except mysql.connector.Error as err:
    print(f"❌ Database error: {err}")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("\\nDatabase connection closed.")
