# 数据库表对比工具

一个用于对比数据库中两个表数据差异的工具，支持 SQLite、MySQL 和 PostgreSQL。

## 功能特点

- 支持多种数据库：SQLite、MySQL、PostgreSQL
- 可以指定要对比的字段或排除特定字段
- 支持添加 WHERE 条件进行过滤
- 智能处理表结构差异
- 详细的差异报告
- 自动处理查询顺序问题，确保结果一致性
- 生成CSV格式的详细差异报告

## 安装

首先克隆或下载此项目，然后安装依赖：

```bash
# 安装基础依赖
pip install -e .

# 对于MySQL支持
pip install mysql-connector-python

# 对于PostgreSQL支持
pip install psycopg2
```

## 使用方法

### 基本用法

安装后，你可以使用简化命令：

```bash
# 对比SQLite数据库中的两个表
table_diff --db-path database.db --table1 users_old --table2 users_new

# 对比MySQL数据库中的两个表
table_diff --db-type mysql --host localhost --port 3306 --user root --password your_password --database your_database --table1 users_old --table2 users_new

# 对比PostgreSQL数据库中的两个表
table_diff --db-type postgresql --host localhost --port 5432 --user postgres --password your_password --database your_database --table1 users_old --table2 users_new
```

或者使用Python脚本方式：

```bash
# 对比SQLite数据库中的两个表
python table_diff.py --db-type sqlite --db-path database.db --table1 users_old --table2 users_new

# 对比MySQL数据库中的两个表
python table_diff.py --db-type mysql --host localhost --port 3306 --user root --password your_password --database your_database --table1 users_old --table2 users_new

# 对比PostgreSQL数据库中的两个表
python table_diff.py --db-type postgresql --host localhost --port 5432 --user postgres --password your_password --database your_database --table1 users_old --table2 users_new
```

### 指定字段对比

```bash
# 只对比指定的字段
table_diff --db-path database.db --table1 users_old --table2 users_new --fields name email age
```

### 排除字段对比

```bash
# 排除特定字段进行对比
table_diff --db-path database.db --table1 users_old --table2 users_new --exclude created_at
```

### 添加WHERE条件

```bash
# 添加WHERE条件进行过滤
table_diff --db-path database.db --table1 users_old --table2 users_new --where "age > 18"
```

### 显示详细差异

```bash
# 显示详细的行差异信息
table_diff --db-path database.db --table1 users_old --table2 users_new --detailed
```

### 生成CSV详细差异报告

```bash
# 生成CSV格式的详细差异报告
table_diff --db-path database.db --table1 users_old --table2 users_new --csv-report differences.csv

# 生成CSV报告并指定字段
table_diff --db-path database.db --table1 users_old --table2 users_new --fields name age --csv-report differences.csv
```

CSV报告将包含以下字段：
- `row_number`: 行号
- `column_name`: 列名
- `table1_value`: 第一个表中的值
- `table2_value`: 第二个表中的值

### 创建示例数据库

```bash
# 创建一个示例数据库用于测试
table_diff --create-sample [--db-path sample.db]
```

## 字段一致性检查

默认情况下，如果两个表的字段不完全一致，工具会停止比对并显示字段差异信息。但有以下例外：

1. 使用 `--fields` 参数指定特定字段时，会忽略字段一致性检查
2. 使用 `--exclude` 参数排除特定字段时，会忽略字段一致性检查

## 查询顺序处理

为确保在所有数据库中查询结果的一致性，工具会自动为查询添加ORDER BY子句：

1. 如果表有主键，会按照主键字段排序
2. 如果表没有主键：
   - 对于PostgreSQL，会按照所有查询字段排序
   - 对于其他数据库，保持查询结果的自然顺序
3. 这确保了在所有支持的数据库中都能正确对比行数据

## 差异报告说明

工具会生成以下类型的差异报告：

1. **行数差异**：两个表的行数不同时会报告
2. **字段差异**：相同行位置但字段值不同时会报告
3. **字段列表**：显示实际用于对比的字段
4. **CSV详细报告**：包含所有差异的详细信息，格式为CSV

## 测试

项目包含全面的测试套件，可以通过以下方式运行：

```bash
# 运行所有测试
python tests/run_tests.py

# 运行特定测试
python tests/test_table_diff.py
```

## 参数说明

### 数据库连接参数

| 参数 | 说明 | 是否必填 |
|------|------|---------|
| --db-type | 数据库类型 (sqlite, mysql, postgresql) | 否，默认为sqlite |
| --db-path | SQLite数据库文件路径 | SQLite必填 |
| --host | 数据库主机地址 | MySQL/PostgreSQL必填 |
| --port | 数据库端口 | 否 (MySQL默认3306, PostgreSQL默认5432) |
| --user | 数据库用户名 | MySQL/PostgreSQL必填 |
| --password | 数据库密码 | MySQL/PostgreSQL必填 |
| --database | 数据库名 | MySQL/PostgreSQL必填 |

### 对比参数

| 参数 | 说明 | 是否必填 |
|------|------|---------|
| --table1 | 第一个表名 | 是 |
| --table2 | 第二个表名 | 是 |
| --fields | 指定要对比的字段（默认对比所有字段） | 否 |
| --exclude | 指定要排除的字段 | 否 |
| --where | WHERE条件 | 否 |
| --detailed | 显示详细差异信息 | 否 |
| --csv-report | 生成CSV格式的详细差异报告到指定文件 | 否 |
| --create-sample | 创建示例数据库 | 否 |

## 示例

### SQLite示例

创建示例数据库：
```bash
table_diff --db-path sample.db --create-sample
```

对比两个用户表的所有字段：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new
```

只对比用户名和邮箱字段：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new --fields name email
```

对比所有字段但排除创建时间字段：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new --exclude created_at
```

只对比年龄大于20的用户：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new --where "age > 20"
```

显示详细差异信息：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new --detailed
```

生成CSV详细差异报告：
```bash
table_diff --db-path sample.db --table1 users_old --table2 users_new --csv-report differences.csv
```

### MySQL示例

```bash
table_diff --db-type mysql --host localhost --port 3306 --user root --password password123 --database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

### PostgreSQL示例

```bash
table_diff --db-type postgresql --host localhost --port 5432 --user postgres --password password123 --database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

## 输出说明

工具会输出以下信息：

1. 对比的字段列表
2. 两个表的记录数
3. 行数差异（如果有的话）
4. 行级数据差异（如果使用了`--detailed`参数）

当发现差异时，工具会显示：
- 行数不同的情况
- 具体每行中哪些字段的值不同（使用`--detailed`参数时）

CSV报告包含所有差异的详细信息，每行一个差异记录，包括行号、列名和两个表中的值。

## 类说明

### TableComparator 类

核心对比类，提供以下方法：

- `set_tables(table1, table2)`: 设置要对比的表
- `set_fields(fields)`: 设置要对比的字段
- `set_exclude_fields(exclude_fields)`: 设置要排除的字段
- `set_where_condition(where_condition)`: 设置WHERE条件
- `compare()`: 执行对比并返回结果
- `generate_csv_report(result, output_file)`: 生成CSV格式的详细差异报告

### DatabaseAdapter 类族

为了支持多种数据库，我们实现了适配器模式：

- `SQLiteAdapter`: SQLite数据库适配器
- `MySQLAdapter`: MySQL数据库适配器
- `PostgreSQLAdapter`: PostgreSQL数据库适配器

## 扩展

该工具通过适配器模式设计，可以轻松扩展以支持更多类型的数据库。只需继承[DatabaseAdapter](diffTableTool\table_diff.py#L13-L21)抽象类并实现相应的方法即可。