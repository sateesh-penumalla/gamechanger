"""
Database Migration: Add Super Order Columns to positions Table

Adds entry_order_id, target_order_id, and sl_order_id columns to track Dhan super order IDs.
"""
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Parse DATABASE_URL
db_url = os.getenv("DATABASE_URL")
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

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    print("\n=== Adding Super Order Columns to positions ===\n")
    
    # Check if columns already exist
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'positions' 
        AND COLUMN_NAME IN ('entry_order_id', 'target_order_id', 'sl_order_id')
    """, (config['database'],))
    
    existing_columns = [row[0] for row in cursor.fetchall()]
    
    cols_to_add = {
        'entry_order_id': 'VARCHAR(50) NULL COMMENT "Dhan order ID for entry"',
        'target_order_id': 'VARCHAR(50) NULL COMMENT "Dhan order ID for target"',
        'sl_order_id': 'VARCHAR(50) NULL COMMENT "Dhan order ID for stop-loss"'
    }
    
    for col, definition in cols_to_add.items():
        if col not in existing_columns:
            print(f"Adding column: {col}...")
            cursor.execute(f"ALTER TABLE positions ADD COLUMN {col} {definition}")
            print(f"✅ Added {col} column")
        else:
            print(f"⏭️  {col} already exists")
    
    conn.commit()
    print("\n✅ Migration completed successfully!")
    
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
