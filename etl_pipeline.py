import pandas as pd
import mysql.connector
from mysql.connector import Error

customers_df = pd.read_csv(r"C:\Users\nada\OneDrive\Desktop\Customer_Sales_ETL\data\olist_customers_dataset.csv")
print("Customers Data : ")
print(customers_df.head())

products_df = pd.read_csv(r"C:\Users\nada\OneDrive\Desktop\Customer_Sales_ETL\data\olist_products_dataset.csv")
print("\nProducts Data:")
print(products_df.head())

sales_df = pd.read_csv(r"C:\Users\nada\OneDrive\Desktop\Customer_Sales_ETL\data\olist_order_items_dataset.csv")
print("\nSales Data:")
print(sales_df.head())

#تنظيف بيانات العملاء
customers_df.drop_duplicates(inplace=True)
customers_df.dropna(subset=['customer_id'], inplace=True)
#تنظيف بيانات المنتجات
products_df.drop_duplicates(inplace=True)
products_df.dropna(subset=['product_id'], inplace=True)

#تنظيف بيانات المبيعات
sales_df.drop_duplicates(inplace=True)
sales_df.dropna(subset=['order_id', 'product_id'], inplace=True)
sales_df['price'] = pd.to_numeric(sales_df['price'], errors='coerce')

print("\nTransformed Customers Data:")
print(customers_df.head())
print("\nTransformed Products Data:")
print(products_df.head())
print("\nTransformed Sales Data:")
print(sales_df.head())

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='Ehsan@2003'
    )

    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS etl_db")
    print("Database 'etl_db' created successfully.")
    cursor.execute("USE etl_db")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_zip_code_prefix INT,
        customer_city VARCHAR(100),
        customer_state VARCHAR(10)
        
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_name_lenght INT,
        product_description_lenght INT
    
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        order_id VARCHAR(50) PRIMARY KEY,
        order_item_id VARCHAR(50),
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        price DECIMAL(10,2)
            );
    """)
    print("Tables created successfully!")

except Error as e:
    print("Error:", e)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")


    
try:
    conn = mysql.connector.connect(
        host='localhost',
        user = 'root',
        password = 'Ehsan@2003',
        database = 'etl_db'
    )

    cursor = conn.cursor()
    print("Connected to MySQL database")

    for i, row in customers_df.iterrows():
        cursor.execute("""
        INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, join_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE customer_unique_id=VALUES(customer_unique_id);
        """, (
            row['customer_id'],
            row['customer_unique_id'],
            row['customer_zip_code_prefix'],
            row['customer_city'],
            row['customer_state'],
            row.get('join_date', None)

        ))

    for i, row in products_df.iterrows():
        cursor.execute("""
        INSERT INTO products (product_id, product_category_name, product_name_lenght, product_description_lenght, price)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE product_category_name=VALUES(product_category_name);
        """, (
            row['product_id'],
            row.get('product_category_name', None),
            row.get('product_name_lenght', None),
            row.get('product_description_lenght', None),
            row.get('price', None)
        ))

    for i, row in sales_df.iterrows():
        cursor.execute("""
        INSERT INTO sales (sale_id, order_id, customer_id, product_id, quantity, price, total_price, sale_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE quantity=VALUES(quantity);
        """, (
            f"{row['order_id']}_{row['order_item_id']}",  # SaleID فريد
            row['order_id'],
            row['customer_id'],
            row['product_id'],
            row.get('quantity', None),
            row.get('price', None),
            row.get('quantity', 0) * row.get('price', 0),  # total_price
            row.get('sale_date', None)
        ))
    conn.commit()
    print("Data loaded successfully into MySQL!")

except Error as e:
    print("Error:", e)

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        print("MySQL connection closed.")


