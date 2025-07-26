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
- 支持跨不同数据库类型比较（如SQLite与MySQL、MySQL与PostgreSQL等）
- 向后兼容：默认情况下源和目标数据库相同
- 当源表和目标表字段不一致时，显示差异并停止比较

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

```
# 对比SQLite数据库中的两个表
table_diff --source-db-path database.db --table1 users_old --table2 users_new

# 对比MySQL数据库中的两个表
table_diff --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# 对比PostgreSQL数据库中的两个表
table_diff --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password your_password --source-database your_database --table1 users_old --table2 users_new
```

或者使用Python脚本方式：

```
# 对比SQLite数据库中的两个表
python table_diff.py --source-db-type sqlite --source-db-path database.db --table1 users_old --table2 users_new

# 对比MySQL数据库中的两个表
python table_diff.py --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password your_password --source-database your_database --table1 users_old --table2 users_new

# 对比PostgreSQL数据库中的两个表
python table_diff.py --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password your_password --source-database your_database --table1 users_old --table2 users_new
```


### 跨数据库比较

现在支持对比不同类型的数据库：

```
# 对比SQLite和MySQL数据库中的表
table_diff --source-db-type sqlite --source-db-path /path/to/local.db \
           --target-db-type mysql --target-host localhost --target-port 3306 \
           --target-user root --target-password your_password --target-database your_database \
           --table1 users --table2 users

# 对比MySQL数据库中的两个表（不同实例）
table_diff --source-db-type mysql --source-host source_host --source-port 3306 \
           --source-user source_user --source-password source_password \
           --source-database source_database \
           --target-db-type mysql --target-host target_host --target-port 3306 \
           --target-user target_user --target-password target_password \
           --target-database target_database \
           --table1 users --table2 users

# 对比MySQL和PostgreSQL数据库中的表
table_diff --source-db-type mysql --source-host mysql_host --source-port 3306 \
           --source-user mysql_user --source-password mysql_password \
           --source-database mysql_database \
           --target-db-type postgresql --target-host pg_host --target-port 5432 \
           --target-user pg_user --target-password pg_password \
           --target-database pg_database \
           --table1 products --table2 products
```

### 指定字段对比

```
# 只对比指定的字段（使用逗号分隔多个字段）
table_diff --source-db-path database.db --table1 users_old --table2 users_new --fields "name,email,age"
```

### 排除字段对比

```
# 排除特定字段进行对比（使用逗号分隔多个字段）
table_diff --source-db-path database.db --table1 users_old --table2 users_new --exclude-fields "created_at,phone"
```

### 添加WHERE条件

```
# 添加WHERE条件进行过滤
table_diff --source-db-path database.db --table1 users_old --table2 users_new --where "age > 18"
```

### 显示详细差异

```
# 显示详细的行差异信息
table_diff --source-db-path database.db --table1 users_old --table2 users_new --detailed
```

### 生成CSV详细差异报告

```
# 生成CSV格式的详细差异报告
table_diff --source-db-path database.db --table1 users_old --table2 users_new --csv-report differences.csv

# 生成CSV报告并指定字段
table_diff --source-db-path database.db --table1 users_old --table2 users_new --fields "name,age" --csv-report differences.csv
```

CSV报告将包含以下字段：
- `row_number`: 行号
- `column_name`: 列名
- `table1_value`: 第一个表中的值
- `table2_value`: 第二个表中的值

### 创建示例数据库

```
# 创建一个示例数据库用于测试
table_diff --create-sample [--source-db-path sample.db]
```

### 字段不一致处理

当两个表的字段不完全一致时，工具会显示字段差异并停止比较过程。这可以帮助您快速识别表结构差异：

```
# 对比两个字段不一致的表时会显示类似以下的输出：
# 表 users_old 和 users_new 字段不一致
# 详细信息包括：
# - 每个表的所有字段
# - 仅在第一个表中存在的字段
# - 仅在第二个表中存在的字段
# - 两个表的公共字段
```

如果您希望在字段不一致的情况下仍然进行比较，可以使用以下方法之一：

1. 使用 `--fields` 参数指定要比较的特定字段：
```
table_diff --source-db-path database.db --table1 users_old --table2 users_new --fields "id,name,email"
```

2. 使用 `--exclude-fields` 参数排除不一致的字段：
```
table_diff --source-db-path database.db --table1 users_old --table2 users_new --exclude-fields "phone,created_at"
```

## 字段一致性检查

默认情况下，如果两个表的字段不完全一致，工具会停止比对并显示字段差异信息。但有以下例外：

1. 使用 `--fields` 参数指定特定字段时，会忽略字段一致性检查
2. 使用 `--exclude-fields` 参数排除特定字段时，会忽略字段一致性检查

当字段不一致时，工具会显示以下信息：
- 两个表各自的完整字段列表
- 仅在第一个表中存在的字段
- 仅在第二个表中存在的字段
- 两个表共有的字段

这有助于用户了解表结构差异并决定如何进行对比。

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

```
# 运行所有测试
python tests/run_tests.py

# 运行特定测试
python tests/test_table_diff.py
```

## 参数说明

### 数据库连接参数

工具支持两种数据库连接配置方式：

1. 单数据库模式（默认）- 源和目标使用同一数据库
2. 双数据库模式 - 可以分别指定源和目标数据库

#
#### 双数据库模式参数

##### 源数据库参数

| 参数 | 说明 | 是否必填 |
|------|------|---------|
| --source-db-type | 源数据库类型 (sqlite, mysql, postgresql) | 否，默认为sqlite |
| --source-db-path | SQLite源数据库文件路径 | SQLite必填 |
| --source-host | 源数据库主机地址 | MySQL/PostgreSQL必填 |
| --source-port | 源数据库端口 | 否 (MySQL默认3306, PostgreSQL默认5432) |
| --source-user | 源数据库用户名 | MySQL/PostgreSQL必填 |
| --source-password | 源数据库密码 | MySQL/PostgreSQL必填 |
| --source-database | 源数据库名 | MySQL/PostgreSQL必填 |

##### 目标数据库参数

| 参数 | 说明 | 是否必填 |
|------|------|---------|
| --target-db-type | 目标数据库类型 (sqlite, mysql, postgresql) | 否，默认与源数据库相同 |
| --target-db-path | SQLite目标数据库文件路径 | 根据需要填写 |
| --target-host | 目标数据库主机地址 | 根据需要填写 |
| --target-port | 目标数据库端口 | 否 |
| --target-user | 目标数据库用户名 | 根据需要填写 |
| --target-password | 目标数据库密码 | 根据需要填写 |
| --target-database | 目标数据库名 | 根据需要填写 |

> 注意：如果未指定目标数据库相关参数，则默认使用源数据库的相关配置。

### 对比参数

| 参数 | 说明 | 是否必填 |
|------|------|---------|
| --table1 | 第一个表名 | 是 |
| --table2 | 第二个表名 | 是 |
| --fields | 指定要对比的字段，多个字段用逗号分隔（默认对比所有字段） | 否 |
| --exclude-fields | 指定要排除的字段，多个字段用逗号分隔 | 否 |
| --where | WHERE条件 | 否 |
| --detailed | 显示详细差异信息 | 否 |
| --csv-report | 生成CSV格式的详细差异报告到指定文件 | 否 |
| --create-sample | 创建示例数据库 | 否 |

## 示例

### SQLite示例

创建示例数据库：
```bash
table_diff --source-db-path sample.db --create-sample
```

对比两个用户表的所有字段：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new
```

只对比用户名和邮箱字段：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --fields "name,email"
```

对比所有字段但排除创建时间字段：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --exclude-fields "created_at"
```

只对比年龄大于20的用户：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --where "age > 20"
```

显示详细差异信息：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --detailed
```

生成CSV详细差异报告：
```bash
table_diff --source-db-path sample.db --table1 users_old --table2 users_new --csv-report differences.csv
```

### MySQL示例

```
table_diff --source-db-type mysql --source-host localhost --source-port 3306 --source-user root --source-password password123 --source-database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

### PostgreSQL示例

```
table_diff --source-db-type postgresql --source-host localhost --source-port 5432 --source-user postgres --source-password password123 --source-database myapp --table1 users_old --table2 users_new --csv-report differences.csv
```

## 输出说明

工具会输出以下信息：

1. 对比的字段列表
2. 两个表的记录数
3. 行数差异（如果有的话）
4. 行级数据差异（如果使用了`--detailed`参数时）

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

### run_comparison 函数

编程接口函数，允许在代码中直接调用表对比功能：

```python
from table_diff import run_comparison

# 基本用法（同一数据库中的表）
result = run_comparison(
    source_db_type='sqlite',
    source_db_path='/path/to/database.db',
    table1='users_old',
    table2='users_new'
)

# 跨数据库比较
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

# 使用字段筛选和CSV报告
result = run_comparison(
    source_db_type='sqlite',
    source_db_path='/path/to/database.db',
    table1='users_old',
    table2='users_new',
    fields=['name', 'email'],
    csv_report='differences.csv'
)
```

### DatabaseAdapter 类族

为了支持多种数据库，我们实现了适配器模式：

- `SQLiteAdapter`: SQLite数据库适配器
- `MySQLAdapter`: MySQL数据库适配器
- `PostgreSQLAdapter`: PostgreSQL数据库适配器

## 扩展

该工具通过适配器模式设计，可以轻松扩展以支持更多类型的数据库。只需继承[DatabaseAdapter](file:///C:/Users/25404/diffTableTool/table_diff.py#L13-L21)抽象类并实现相应的方法即可.