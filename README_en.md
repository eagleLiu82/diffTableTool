# Database Table Comparison Tool

A powerful tool for comparing data differences between two tables in a database, supporting SQLite, MySQL, PostgreSQL, Oracle, and MSSQL.

## Features

- Multi-database support: SQLite, MySQL, PostgreSQL, Oracle, MSSQL
- Field specification or exclusion for comparison
- WHERE condition filtering
- Intelligent handling of table structure differences
- Detailed difference reports
- Automatic handling of query ordering issues to ensure result consistency
- CSV format detailed difference reports
- Cross-database comparison support (e.g., SQLite vs MySQL, MySQL vs PostgreSQL, Oracle vs MSSQL, etc.)
- Backward compatibility: by default, source and target databases are the same
- Field mismatch detection with comparison stopping when source and target table fields don't match
- Both Graphical User Interface (GUI) and Command Line Interface (CLI) support
- GUI defaults to PostgreSQL database type
- In GUI, source database configuration automatically syncs to target database configuration (unless manually modified)

## Installation

First, clone or download this project, then install dependencies:

```bash
# For MySQL support
pip install mysql-connector-python

# For PostgreSQL support
pip install psycopg2

# For Oracle support
pip install oracledb

# For MSSQL support
pip install pymssql
```

## Usage

### Graphical Interface

To start the GUI version, you can use either of the following methods:

```bash
# Start GUI with --gui parameter
table_diff --gui

# Or start GUI using Python script
python table_diff.py --gui
```

The graphical interface provides an intuitive operation mode with the following features:

1. Default database type is PostgreSQL
2. Source database configuration automatically syncs to target database configuration, simplifying input for same database configurations
3. Convenient configuration of database connection parameters, table names, fields, and other options
4. Supports switching between different tabs while preserving entered content
5. View comparison results
6. Supports saving and loading configuration files (JSON format)

### Command Line Interface

#### Basic Usage

After installation, you can use the simplified command:

```bash
# Compare two tables in SQLite database
table_diff --source-db-path database.db --table1 users_old --table2 users_new

# Compare two tables in MySQL database
table_diff --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# Compare two tables in PostgreSQL database
table_diff --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# Compare two tables in Oracle database
table_diff --source-db-type oracle --source-host localhost --source-port 1521 --source-user system --source-password your_password --source-database ORCL --table1 users_old --table2 users_new

# Compare two tables in MSSQL database
table_diff --source-db-type mssql --source-host localhost --source-port 1433 --source-user sa --source-password your_password --source-database your_database --table1 users_old --table2 users_new
```

Or using the Python script method:

```bash
# Compare two tables in SQLite database
python table_diff.py --source-db-type sqlite --source-db-path database.db --table1 users_old --table2 users_new

# Compare two tables in MySQL database
python table_diff.py --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# Compare two tables in PostgreSQL database
python table_diff.py --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# Compare two tables in Oracle database
python table_diff.py --source-db-type oracle --source-host localhost --source-port 1521 --source-user system --source-password your_password --source-database ORCL --table1 users_old --table2 users_new

# Compare two tables in MSSQL database
python table_diff.py --source-db-type mssql --source-host localhost --source-port 1433 --source-user sa --source-password your_password --source-database your_database --table1 users_old --table2 users_new
```

### Cross-Database Comparison

Now supports comparing tables across different database types:

```bash
# Compare tables between SQLite and MySQL databases
table_diff --source-db-type sqlite --source-db-path /path/to/local.db \
           --target-db-type mysql --target-host localhost --target-port 3306 \
           --target-user root --target-password your_password --target-database your_database \
           --table1 users --table2 users

# Compare tables between two MySQL databases (different instances)
table_diff --source-db-type mysql --source-host source_host --source-port 3306 \
           --source-user source_user --source-password source_password \
           --source-database source_database \
           --target-db-type mysql --target-host target_host --target-port 3306 \
           --target-user target_user --target-password target_password \
           --target-database target_database \
           --table1 users --table2 users

# Compare tables between MySQL and PostgreSQL databases
table_diff --source-db-type mysql --source-host mysql_host --source-port 3306 \
           --source-user mysql_user --source-password mysql_password \
           --source-database mysql_database \
           --target-db-type postgresql --target-host pg_host --target-port 5432 \
           --target-user pg_user --target-password pg_password \
           --target-database pg_database \
           --table1 products --table2 products

# Compare tables between Oracle and MSSQL databases
table_diff --source-db-type oracle --source-host oracle_host --source-port 1521 \
           --source-user oracle_user --source-password oracle_password \
           --source-database oracle_database \
           --target-db-type mssql --target-host mssql_host --target-port 1433 \
           --target-user mssql_user --target-password mssql_password \
           --target-database mssql_database \
           --table1 employees --table2 employees
```

### Field-Specific Comparison

```bash
# Compare only specified fields (use comma to separate multiple fields)
table_diff --source-db-path database.db --table1 users_old --table2 users_new --fields "name,email,age"
```

### Exclude Fields from Comparison

```bash
# Exclude specific fields from comparison
table_diff --source-db-path database.db --table1 users_old --table2 users_new --exclude "created_at,updated_at"
```

### Add WHERE Conditions

```bash
# Add WHERE condition for filtering
table_diff --source-db-path database.db --table1 users_old --table2 users_new --where "age > 18"
```

### Show Detailed Differences

```bash
# Show detailed row difference information
table_diff --source-db-path database.db --table1 users_old --table2 users_new --detailed
```

### Generate Detailed Report

```bash
# Generate CSV format detailed difference report
table_diff --source-db-path database.db --table1 users_old --table2 users_new --detailed --csv-report report.csv
```

The CSV report will include the following fields:
- `row_number`: Row number
- `column_name`: Column name
- `table1_value`: Value in the first table
- `table2_value`: Value in the second table

### Create Sample Database

```bash
# Create sample database for testing
table_diff --create-sample --source-db-path sample.db
```

### Field Inconsistency Handling

When two tables don't have exactly the same fields, the tool will display field differences and stop the comparison process. This helps you quickly identify structural differences:

```bash
# When comparing two tables with inconsistent fields, output similar to the following will be displayed:
# Table users_old and users_new have inconsistent fields
# Detailed information includes:
# - All fields in each table
# - Fields that exist only in the first table
# - Fields that exist only in the second table
# - Common fields in both tables
```

If you want to compare even when fields are inconsistent, you can use one of the following methods:

1. Use the `--fields` parameter to specify particular fields to compare:
```bash
table_diff --source-db-path database.db --table1 users_old --table2 users_new --fields "id,name,email"
```

2. Use the `--exclude-fields` parameter to exclude inconsistent fields:
```bash
table_diff --source-db-path database.db --table1 users_old --table2 users_new --exclude-fields "phone,created_at"
```

## Field Consistency Check

By default, if two tables don't have exactly the same fields, the tool will stop comparison and display field difference information. However, there are exceptions:

1. When using the `--fields` parameter to specify particular fields, field consistency checking is ignored
2. When using the `--exclude-fields` parameter to exclude specific fields, field consistency checking is ignored

When fields are inconsistent, the tool will display the following information:
- Complete field list for both tables
- Fields that exist only in the first table
- Fields that exist only in the second table
- Common fields in both tables

This helps users understand structural differences and decide how to proceed with comparison.

## Query Ordering Handling

To ensure consistent query results across all databases, the tool automatically adds ORDER BY clauses to queries:

1. If the table has a primary key, it will be ordered by primary key fields
2. If the table doesn't have a primary key:
   - For PostgreSQL, it will be ordered by all query fields
   - For other databases, the query results maintain their natural order
3. This ensures correct row data comparison across all supported databases

## Difference Report Explanation

The tool generates the following types of difference reports:

1. **Row count differences**: Reported when the two tables have different row counts
2. **Field differences**: Reported when same-position rows have different field values
3. **Field list**: Shows the actual fields used for comparison
4. **CSV detailed report**: Contains detailed information about all differences in CSV format

## Testing

The project includes a comprehensive test suite that can be run as follows:

```bash
# Run all tests
python tests/run_tests.py

# Run specific test
python tests/test_table_diff.py
```

## Parameter Description

### Database Connection Parameters

The tool supports two database connection configuration methods:

1. Single database mode (default) - Source and target use the same database
2. Dual database mode - Source and target databases can be specified separately

### Dual Database Mode Parameters

#### Source Database Parameters

| Parameter | Description | Required |
|-----------|-------------|----------|
| --source-db-type | Source database type (sqlite, mysql, postgresql, oracle, mssql) | No, defaults to sqlite |
| --source-db-path | SQLite source database file path | Required for SQLite |
| --source-host | Source database host address | Required for MySQL/PostgreSQL/Oracle/MSSQL |
| --source-port | Source database port | No (MySQL defaults to 3306, PostgreSQL defaults to 5432, Oracle defaults to 1521, MSSQL defaults to 1433) |
| --source-user | Source database username | Required for MySQL/PostgreSQL/Oracle/MSSQL |
| --source-password | Source database password | Required for MySQL/PostgreSQL/Oracle/MSSQL |
| --source-database | Source database name | Required for MySQL/PostgreSQL/Oracle/MSSQL |
| --source-service-name | Oracle source database service name | Optional for Oracle |

#### Target Database Parameters

| Parameter | Description | Required |
|----------|-------------|----------|
| --target-db-type | Target database type (sqlite, mysql, postgresql, oracle, mssql) | No, defaults to same as source database |
| --target-db-path | SQLite target database file path | Fill as needed |
| --target-host | Target database host address | Fill as needed |
| --target-port | Target database port | No |
| --target-user | Target database username | Fill as needed |
| --target-password | Target database password | Fill as needed |
| --target-database | Target database name | Fill as needed |
| --target-service-name | Oracle target database service name | Optional for Oracle |

> Note: If target database parameters are not specified, the tool defaults to using the source database configuration.

### Comparison Parameters

| Parameter | Description | Required |
|----------|-------------|----------|
| --table1 | First table name | Yes |
| --table2 | Second table name | Yes |
| --fields | Fields to compare, multiple fields separated by commas (defaults to all fields) | No |
| --exclude-fields | Fields to exclude, multiple fields separated by commas | No |
| --where | WHERE condition | No |
| --detailed | Show detailed difference information | No |
| --csv-report | Generate CSV format detailed difference report to specified file | No |
| --create-sample | Create sample database | No |

## Examples

### SQLite Examples

Create sample database:
```bash
table_diff --source-db-path sample.db --create-sample
```

Compare all fields of two user tables:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new
```

Compare only username and email fields:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --fields "name,email"
```

Compare all fields but exclude creation time field:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --exclude-fields "created_at"
```

Compare only users with age greater than 20:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --where "age > 20"
```

Show detailed difference information:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --detailed
```

Generate CSV detailed difference report:
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --csv-report differences.csv
```

### MySQL Example

```bash
table_diff --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password password123 --source-database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

### PostgreSQL Example

```bash
table_diff --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password password123 --source-database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

### Oracle Example

```bash
# Connect to Oracle database using SID
table_diff --source-db-type oracle --source-host localhost --source-port 1521 --source-user system --source-password password --source-database ORCL --table1 employees_2022 --table2 employees_2023

# Connect to Oracle database using Service Name
table_diff --source-db-type oracle --source-host localhost --source-port 1521 --source-user system --source-password password --source-service-name ORCLSERVICE --table1 employees_2022 --table2 employees_2023

# Cross-database comparison: Oracle and PostgreSQL
table_diff --source-db-type oracle --source-host oracle-host --source-port 1521 --source-user system --source-password oracle-password --source-database ORCL --table1 products \
           --target-db-type postgresql --target-host pg-host --target-port 5432 --target-user postgres --target-password pg-password --target-database myapp --table2 products
```

### MSSQL Example

```bash
# Connect to MSSQL database
table_diff --source-db-type mssql --source-host localhost --source-port 1433 --source-user sa --source-password StrongPassword123 --source-database TestDB --table1 employees_2022 --table2 employees_2023

# Cross-database comparison: MSSQL and MySQL
table_diff --source-db-type mssql --source-host mssql-host --source-port 1433 --source-user sa --source-password mssql-password --source-database TestDB --table1 users \
           --target-db-type mysql --target-host mysql-host --target-port 3306 --target-user root --target-password mysql-password --target-database myapp --table2 users
```

## Output Explanation

The tool outputs the following information:

1. List of fields being compared
2. Record counts of both tables
3. Row count differences (if any)
4. Row-level data differences (if `--detailed` parameter is used)

When differences are found, the tool displays:
- Row count differences
- Specific fields with different values in each row (when using `--detailed` parameter)

The CSV report contains detailed information about all differences, with one difference record per row, including row number, column name, and values in both tables.

## Class Description

### TableComparator Class

Core comparison class, providing the following methods:

- `set_tables(table1, table2)`: Set tables to compare
- `set_fields(fields)`: Set fields to compare
- `set_exclude_fields(exclude_fields)`: Set fields to exclude
- `set_where_condition(where_condition)`: Set WHERE condition
- `compare()`: Execute comparison and return results
- `generate_csv_report(result, output_file)`: Generate CSV format detailed difference report

### run_comparison Function

Programming interface function that allows direct calling of table comparison functionality in code:

```python
from table_diff import run_comparison

# Basic usage (tables in same database)
result = run_comparison(
    source_db_type='sqlite',
    source_db_path='/path/to/database.db',
    table1='users_old',
    table2='users_new'
)

# Cross-database comparison
result = run_comparison(
    source_db_type='sqlite',
    source_db_path='/path/to/source.db',
    target_db_type='mysql',
    target_host='localhost',
    target_port=3306,
    target_user='user',
    target_password='password',
    target_database='database',
    table1='users',
    table2='users'
)

# Using field filtering and CSV report
result = run_comparison(
    source_db_type='sqlite',
    source_db_path='/path/to/database.db',
    table1='users_old',
    table2='users_new',
    fields=['name', 'email'],
    csv_report='differences.csv'
)
```

### DatabaseAdapter Class Family

To support multiple databases, we implement the adapter pattern:

- `SQLiteAdapter`: SQLite database adapter
- `MySQLAdapter`: MySQL database adapter
- `PostgreSQLAdapter`: PostgreSQL database adapter
- `OracleAdapter`: Oracle database adapter
- `MSSQLAdapter`: MSSQL database adapter

## Extensibility

The tool is designed with the adapter pattern, making it easy to extend support for more database types. Simply inherit the [DatabaseAdapter](file:///C:/Users/25404/diffTableTool/table_diff.py#L13-L21) abstract class and implement the corresponding methods.

## Supported Databases

- SQLite
- MySQL
- PostgreSQL
- Oracle (requires `oracledb` package)
- MSSQL (requires `pymssql` package)

### Installing Additional Dependencies

Depending on database types, you may need to install additional dependency packages:

```bash
# MySQL support
pip install mysql-connector-python

# PostgreSQL support
pip install psycopg2

# Oracle support
pip install oracledb

# MSSQL support
pip install pymssql
```

Alternatively, you can use pip's extras feature to install support for specific databases:

```bash
# Install MySQL support
pip install table-diff-tool[mysql]

# Install PostgreSQL support
pip install table-diff-tool[postgresql]

# Install Oracle support
pip install table-diff-tool[oracle]

# Install MSSQL support
pip install table-diff-tool[mssql]

# Install support for all databases
pip install table-diff-tool[mysql,postgresql,oracle,mssql]
```