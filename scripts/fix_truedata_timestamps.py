#!/usr/bin/env python3
"""
Script to convert TRUEDATA timestamps from GMT to IST (GMT+5:30)
This fixes historical records that were stored with GMT timestamps.
Handles duplicates by removing GMT records that conflict with IST records.
"""

import os
import sys
from datetime import timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def fix_truedata_timestamps():
    """Update all TRUEDATA records to convert GMT timestamps to IST."""
    
    # Get database URL
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        sys.exit(1)
    
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # Get count of TRUEDATA records to update
                count_query = text("""
                    SELECT COUNT(*) as count 
                    FROM intraday_ticks 
                    WHERE source = 'TRUEDATA' 
                    AND timestamp < '2026-02-13 09:00:00'
                """)
                result = conn.execute(count_query)
                count = result.fetchone()[0]
                
                print(f"Found {count} TRUEDATA records with GMT timestamps to process...")
                
                if count == 0:
                    print("No records to update. Exiting.")
                    return
                
                # Step 1: Delete GMT records that would conflict with existing IST records
                print("\nStep 1: Removing GMT records that conflict with existing IST records...")
                delete_query = text("""
                    DELETE t1 FROM intraday_ticks t1
                    INNER JOIN intraday_ticks t2 
                    ON t1.symbol = t2.symbol 
                    AND DATE_ADD(t1.timestamp, INTERVAL 330 MINUTE) = t2.timestamp
                    AND t1.source = 'TRUEDATA'
                    AND t2.source = 'TRUEDATA'
                    WHERE t1.timestamp < '2026-02-13 09:00:00'
                """)
                
                result = conn.execute(delete_query)
                deleted_count = result.rowcount
                print(f"   Deleted {deleted_count} conflicting GMT records")
                
                # Step 2: Update remaining GMT timestamps to IST
                print("\nStep 2: Converting remaining GMT timestamps to IST...")
                update_query = text("""
                    UPDATE intraday_ticks 
                    SET timestamp = DATE_ADD(timestamp, INTERVAL 330 MINUTE),
                        last_updated = NOW()
                    WHERE source = 'TRUEDATA' 
                    AND timestamp < '2026-02-13 09:00:00'
                """)
                
                result = conn.execute(update_query)
                rows_updated = result.rowcount
                
                # Commit transaction
                trans.commit()
                
                print(f"   Updated {rows_updated} TRUEDATA records from GMT to IST")
                print(f"\n✅ Total processed: {deleted_count} deleted + {rows_updated} updated = {deleted_count + rows_updated} records")
                
                # Show sample of updated records
                sample_query = text("""
                    SELECT symbol, timestamp, close, source, last_updated
                    FROM intraday_ticks 
                    WHERE source = 'TRUEDATA'
                    ORDER BY timestamp DESC
                    LIMIT 5
                """)
                
                print("\nSample of TRUEDATA records after conversion:")
                print("-" * 80)
                result = conn.execute(sample_query)
                for row in result:
                    print(f"{row.symbol:12} | {row.timestamp} | {row.close:7.2f} | {row.source:10} | {row.last_updated}")
                
            except Exception as e:
                trans.rollback()
                print(f"❌ Error during update: {e}")
                raise
                
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        sys.exit(1)
    
    print("\n✅ Timestamp conversion completed successfully!")

if __name__ == "__main__":
    print("=" * 80)
    print("TRUEDATA Timestamp Converter (GMT → IST)")
    print("=" * 80)
    print()
    
    # Confirm before proceeding
    response = input("This will update all TRUEDATA timestamps from GMT to IST. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Aborted.")
        sys.exit(0)
    
    fix_truedata_timestamps()
