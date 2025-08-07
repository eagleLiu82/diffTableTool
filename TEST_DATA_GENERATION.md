# MySQL测试数据生成指南

本文档介绍如何使用脚本向MySQL数据库中生成大量测试数据，用于测试数据库表对比工具处理大数据量的能力。

## 准备工作

### 1. 安装依赖

确保已安装MySQL连接器：

```bash
pip install mysql-connector-python
```

### 2. 配置数据库连接

编辑 [mysql_config.py](file:///c%3A/Users/25404/diffTableTool/mysql_config.py) 文件，配置您的MySQL数据库连接参数：

```python
MYSQL_CONFIG = {
    'host': 'localhost',      # 数据库主机地址
    'port': 3306,             # 数据库端口
    'user': 'root',           # 用户名
    'password': 'password',   # 密码
    'database': 'test_db'     # 数据库名
}
```

### 3. 创建数据库

在MySQL中创建相应的数据库：

```sql
CREATE DATABASE test_db;
```

## 使用脚本生成测试数据

### 运行数据生成脚本

```bash
python generate_mysql_test_data.py
```

脚本将执行以下操作：
1. 连接到MySQL数据库
2. 创建两个测试表：[employees_2022](file:///c%3A/Users/25404/diffTableTool/tests/TEST_DATABASE.md#L25-L33) 和 [employees_2023](file:///c%3A/Users/25404/diffTableTool/tests/TEST_DATABASE.md#L35-L45)
3. 向每个表中插入10万行测试数据

### 数据表结构

#### employees_2022 表
```sql
CREATE TABLE employees_2022 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary NUMERIC(10, 2),
    hire_date DATE,
    is_active BOOLEAN
);
```

#### employees_2023 表
```sql
CREATE TABLE employees_2023 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary NUMERIC(10, 2),
    hire_date DATE,
    phone VARCHAR(20),
    is_active BOOLEAN,
    last_login TIMESTAMP
);
```

## 使用表对比工具测试

数据生成完成后，可以使用表对比工具测试大数据量处理能力：

```bash
python table_diff.py \
  --source-db-type mysql \
  --source-host localhost \
  --source-port 3306 \
  --source-user root \
  --source-password password \
  --source-database test_db \
  --table1 employees_2022 \
  --table2 employees_2023 \
  --fields id,name,email,department,salary,hire_date,is_active
```

## 性能提示

1. 插入过程使用批量插入，每批1000行，以提高插入效率
2. 每插入1000行会提交一次事务，以平衡性能和数据安全性
3. 插入过程中会显示进度信息
4. 如果需要调整数据量，可以修改 [generate_mysql_test_data.py](file:///c%3A/Users/25404/diffTableTool/generate_mysql_test_data.py) 中的 `count` 参数

## 注意事项

1. 确保MySQL服务正在运行
2. 确保配置的用户有足够的权限创建表和插入数据
3. 插入10万行数据可能需要几分钟时间，请耐心等待
4. 如果需要重新生成数据，请先清空或删除现有表
5. 生成的数据是随机的，每次运行都会生成不同的数据

## 清理数据

如需清理测试数据，可以执行以下SQL语句：

```sql
DROP TABLE employees_2022;
DROP TABLE employees_2023;
```