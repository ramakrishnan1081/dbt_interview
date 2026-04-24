import duckdb
import pandas as pd
import os

conn = duckdb.connect('dbt.duckdb')

# Get all tables
all_tables = conn.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    ORDER BY table_schema, table_name
""").fetchall()

# Query rows from each table that matches dim_* or fct_* in marts schema
marts_tables = [(s, t) for s, t in all_tables if s == 'marts' and (t.startswith('dim_') or t.startswith('fct_'))]

output_dir = '/workspaces/dbt_interview/marts_csv'
os.makedirs(output_dir, exist_ok=True)

print("="*80)
print("EXPORTING MARTS MODELS TO CSV")
print("="*80)

for schema, table_name in sorted(marts_tables):
    full_table_name = f"{schema}.{table_name}"
    csv_filename = f"{table_name}.csv"
    csv_filepath = os.path.join(output_dir, csv_filename)
    
    try:
        df = conn.execute(f"SELECT * FROM {full_table_name}").df()
        df.to_csv(csv_filepath, index=False)
        print(f"✓ {csv_filename:<30} ({len(df):>2} rows, {len(df.columns):>2} cols)")
    except Exception as e:
        print(f"✗ {csv_filename:<30} Error: {e}")

conn.close()
print("\n" + "="*80)
print(f"CSV files saved to: {output_dir}")
print("="*80)

