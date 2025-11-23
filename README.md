# Customer Sales ETL Project

## **Project Overview**

This project is a complete ETL (Extract, Transform, Load) pipeline for customer, product, and sales data.
It demonstrates the process of extracting data from CSV files, cleaning and transforming it using Python and Pandas, and loading it into a MySQL database.

---

## **Tools and Technologies**

* Python
* Pandas
* MySQL
* CSV files
* Jupyter Notebook (optional)
* PyCharm (IDE)

---

## **Project Structure**

```
Customer_Sales_ETL/
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── sales.csv
│
├── etl_pipeline.py
├── README.md
└── requirements.txt
```

---

## **ETL Process**

### **1. Extract**

* Read CSV files using Pandas:

  * `customers.csv`
  * `products.csv`
  * `sales.csv`
* Display the first few rows to verify successful extraction.

### **2. Transform**

* Remove duplicates from all tables.
* Handle missing or null values.
* Convert dates to `datetime` format.
* Convert numeric columns to appropriate types.
* Create a new column `total_price = quantity * price` in sales data.

### **3. Load**

* Connect to MySQL database (`etl_db`).
* Create tables if not exist: `customers`, `products`, `sales`.
* Insert data from DataFrames into the corresponding tables.
* Use `ON DUPLICATE KEY UPDATE` to avoid duplicates.

---

## **Sample Data Preview**

**Customers Data**

| customer_id                      | customer_unique_id | customer_zip_code_prefix | customer_city | customer_state |
| -------------------------------- | ------------------ | ------------------------ | ------------- | -------------- |
| 06b8999e2fba1a1fbc88172c00ba8bc7 | ...                | 01000                    | São Paulo     | SP             |
| 18955e83d337fd6b2def6b18a428ac77 | ...                | 01001                    | São Paulo     | SP             |

**Products Data**

| product_id                       | product_category_name | product_name_lenght | product_description_lenght | price  |
| -------------------------------- | --------------------- | ------------------- | -------------------------- | ------ |
| 1e9e8ef04dbcff4541ed26657ea517e5 | Electronics           | 50                  | 300                        | 58.90  |
| 3aa071139cb16b67ca9e5dea641aaa2f | Electronics           | 60                  | 400                        | 239.90 |

**Sales Data**

| sale_id                            | order_id                         | customer_id                      | product_id                       | quantity | price | total_price | sale_date  |
| ---------------------------------- | -------------------------------- | -------------------------------- | -------------------------------- | -------- | ----- | ----------- | ---------- |
| 00010242fe8c5a6d1ba2dd792cb16214_1 | 00010242fe8c5a6d1ba2dd792cb16214 | 06b8999e2fba1a1fbc88172c00ba8bc7 | 1e9e8ef04dbcff4541ed26657ea517e5 | 1        | 58.90 | 58.90       | 2020-01-01 |

---

## **How to Run**

1. Install required packages:

```bash
pip install pandas mysql-connector-python
```

2. Open `etl_pipeline.py` in PyCharm or Jupyter Notebook.
3. Adjust MySQL connection settings (`host`, `user`, `password`).
4. Run the script to execute the full ETL process.

---

## **Outcome**

* Data is successfully loaded into MySQL tables: `customers`, `products`, `sales`.
* Data can be queried, analyzed, or used for dashboards and analytics.

---

## **Author**

Nada Khadim

University of Al-Zaytoonah, AI Department, 2025
