import yaml
from pathlib import Path

SOURCE_PATH="/Volumes/ecommerce/default/raw_data"

BRONZE_TABLES={
    "customers":"ecommerce.default.bronze_customers",
    "products":"ecommerce.default.bronze_products",
    "orders":"ecommerce.default.bronze_orders",
    "order_items":"ecommerce.default.bronze_order_items",
    "payments":"ecommerce.default.bronze_payments",
    "shipments":"ecommerce.default.bronze_shipments",
    "returns":"ecommerce.default.bronze_returns"
}

SILVER_TABLES={
    "customers":"ecommerce.default.silver_customers",
    "products":"ecommerce.default.silver_products",
    "orders":"ecommerce.default.silver_orders",
    "order_items":"ecommerce.default.silver_order_items",
    "payments":"ecommerce.default.silver_payments",
    "shipments":"ecommerce.default.silver_shipments",
    "returns":"ecommerce.default.silver_returns"
}

GOLD_TABLES={
    "fact_order_items": "ecommerce.default.fact_order_items",
    "dim_products": "ecommerce.default.dim_products",
    "dim_customers": "ecommerce.default.dim_customers",
    "dim_date": "ecommerce.default.dim_date"
}

def load_bronze_config()->dict:
    config_path=Path(__file__).parent.parent/"config"/"bronze_tables.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
    

