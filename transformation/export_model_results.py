#!/usr/bin/env python3
"""
Export results from all dbt models to CSV files in a model_results folder.
Run this after executing 'dbt build'.
"""

import os
import json
import duckdb
from pathlib import Path

# Configuration
DB_PATH = "dbt.duckdb"
OUTPUT_DIR = "model_results"
SCHEMA = "marts"  # Change to "marts", "intermediate", "staging" as needed

def create_output_directory():
    """Create the output directory if it doesn't exist."""
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    print(f"✓ Output directory created: {OUTPUT_DIR}/")

def get_models_from_manifest():
    """Extract model names from manifest.json."""
    manifest_path = "target/manifest.json"
    
    if not os.path.exists(manifest_path):
        print(f"Error: {manifest_path} not found. Run 'dbt build' first.")
        return []
    
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
    
    models = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            model_name = node.get("name")
            if model_name:
                models.append(model_name)
    
    return sorted(models)

def query_fetch_all(conn, query):
    """Execute a query and fetch all results."""
    return conn.execute(query).fetchall()

def export_model_data(conn, model_name, schema):
    """Export a model's data to CSV."""
    try:
        # Query the model table
        table_name = f'"{schema}"."{model_name}"'
        query = f"SELECT * FROM {table_name}"
        
        result = conn.execute(query).fetchall()
        
        if not result:
            print(f"  ⚠ No data found")
            return False
        
        # Export to CSV
        output_file = os.path.join(OUTPUT_DIR, f"{model_name}.csv")
        conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_file}' (FORMAT CSV, HEADER)")
        
        row_count = len(result)
        print(f"  ✓ Exported {row_count} rows to {model_name}.csv")
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {str(e)}")
        return False

def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("DBT Model Results Exporter")
    print("="*60 + "\n")
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found. Make sure you're in the transformation directory.")
        return
    
    # Create output directory
    create_output_directory()
    
    # Get models from manifest
    models = get_models_from_manifest()
    
    if not models:
        print("No models found in manifest.json")
        return
    
    print(f"\nFound {len(models)} models. Exporting results...\n")
    
    # Connect to database
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    try:
        # Get available schemas
        schemas_query = "SELECT DISTINCT table_schema FROM information_schema.tables ORDER BY table_schema"
        available_schemas = [row[0] for row in conn.execute(schemas_query).fetchall()]
        print(f"Available schemas: {', '.join(available_schemas)}\n")
        
        # Export each model
        exported_count = 0
        for model in models:
            # Try to find the model in any schema
            found = False
            for schema in available_schemas:
                if schema == 'information_schema' or schema.startswith('dbt_test'):
                    continue
                    
                try:
                    # Check if table exists in this schema
                    check_query = f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{model}'"
                    if conn.execute(check_query).fetchone():
                        print(f"Exporting: {model} (from {schema})")
                        if export_model_data(conn, model, schema):
                            exported_count += 1
                        found = True
                        break
                except:
                    continue
            
            if not found:
                print(f"⚠ Model not found in any schema: {model}")
        
        print(f"\n{'='*60}")
        print(f"Export complete: {exported_count}/{len(models)} models exported")
        print(f"Results saved to: {os.path.abspath(OUTPUT_DIR)}")
        print(f"{'='*60}\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
