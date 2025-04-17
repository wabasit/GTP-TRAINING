# Inventory and Order Management System

This project is a SQL Server–based solution designed to manage inventory, customers, and order processing for an e-commerce company. It ensures accurate tracking of stock levels, automates order handling, and provides meaningful insights into business operations and customer behavior.

---

## Project Overview

The system handles the core functions of an e-commerce backend, including:

- Managing product details and stock quantities
- Tracking customer information and order histories
- Logging all inventory changes in a dedicated audit table
- Automatically replenishing stock when it falls below a reorder threshold
- Categorizing customers based on their total spending
- Generating summary reports and business insights

---

## Project Phases

### Phase 1: Database Design and Schema Implementation

- Created normalized tables for:
  - Products
  - Customers
  - Orders
  - Order Details
  - Inventory Logs
- Enforced data integrity using primary and foreign key constraints

### Phase 2: Order Placement and Inventory Management

- Implemented stored procedures to:
  - Place customer orders
  - Calculate total order value
  - Deduct stock levels accordingly
  - Update inventory logs
- Designed logic to support multiple items per order

### Phase 3: Monitoring and Reporting

- Built queries to:
  - Display customer order summaries (date, amount, quantity)
  - Identify products low on stock
  - Track total customer spending and classify them into tiers (Bronze, Silver, Gold)
- Applied bulk discounts based on order quantity

### Phase 4: Stock Replenishment and Automation

- Created a replenishment procedure to:
  - Detect products below reorder level
  - Replenish stock automatically
  - Log the inventory changes
- Automated key processes such as:
  - Stock deduction after orders
  - Order value calculations
  - Customer spending tier assignment

### Phase 5: Advanced Queries and Optimizations

- Created reusable views for:
  - Order summary (vw_OrderSummary): customer name, order date, total, items
  - Stock status (vw_LowStock): low-stock products needing replenishment
- Added indexing strategies to enhance performance with growing data [idx_orders_customer_id, idx_orderdetails_order_id, idx_orderdetails_product_id, idx_products_stock_quantity]

---

## Getting Started

### Prerequisites

- [SQL Server](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) (Express)
- [SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms)
- Git installed on your system (https://git-scm.com/downloads)

---

### Clone the Repository

Open your terminal or Git Bash and run:

```bash
git clone https://github.com/wabasit/GTP-TRAINING/tree/mylab2/inventory-order-management
cd inventory-order-management
```

## Usage Instructions

#### To run stored procedures to simulate real operations:

```bash
EXEC PlaceOrder @customer_id = 1, @product_id = 2, @quantity = 3;
```
```bash
EXEC ReplenishStock;
``` 

#### Query customer tier classification:

```bash
SELECT * FROM vw_CustomerSpendingTier;
```

#### Check orders summary and stock status:
```bash
SELECT * FROM vw_OrderSummary;
```
```bash
SELECT * FROM vw_LowStock;
```

## Author:
This project was developed as part of a database systems training program by A.W. Basit. It reflects real-world business scenarios in inventory and order management using relational databases.