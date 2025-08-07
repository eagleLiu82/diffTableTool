# PostgreSQL测试数据生成说明

本文档介绍了如何使用脚本向PostgreSQL数据库中的两个表各插入10万行测试数据。

## 文件列表

1. [generate_postgresql_test_data.py](file:///c%3A/Users/25404/diffTableTool/generate_postgresql_test_data.py) - 数据生成脚本
2. [postgresql_config.py](file:///c%3A/Users/25404/diffTableTool/postgresql_config.py) - PostgreSQL数据库配置文件

## 表结构

### employees_2022表
```sql
CREATE TABLE employees_2022 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary NUMERIC(10, 2),
    hire_date DATE,
    is_active BOOLEAN
);
```

### employees_2023表
```sql
CREATE TABLE employees_2023 (
    id SERIAL PRIMARY KEY,
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

## 使用方法

### 1. 安装依赖

首先确保已安装PostgreSQL的Python驱动：

```bash
pip install psycopg2
```

### 2. 配置数据库连接

编辑[postgresql_config.py](file:///c%3A/Users/25404/diffTableTool/postgresql_config.py)文件，修改数据库连接参数：

```python
POSTGRESQL_CONFIG = {
    'host': 'localhost',        # PostgreSQL服务器地址
    'port': 5432,               # PostgreSQL端口
    'user': 'postgres',         # 用户名
    'password': 'your_password', # 密码
    'database': 'test_db'       # 数据库名
}
```

### 3. 创建数据库

在PostgreSQL中创建数据库：

```sql
CREATE DATABASE test_db;
```

### 4. 运行数据生成脚本

```bash
python generate_postgresql_test_data.py
```

脚本将执行以下操作：
1. 连接到PostgreSQL数据库
2. 删除已存在的employees_2022和employees_2023表
3. 创建新的employees_2022和employees_2023表
4. 向每个表中插入10万行测试数据

## 数据特征

生成的测试数据具有以下特征：

1. **姓名**: 随机生成中文姓名
2. **邮箱**: 基于姓名和索引生成的随机邮箱地址
3. **部门**: 从预定义列表中随机选择部门
4. **薪资**: 30000到150000之间的随机数值
5. **入职日期**: 2020-01-01到2023-12-31之间的随机日期
6. **电话**: 随机生成的中国手机号码格式
7. **是否激活**: 随机布尔值
8. **最后登录时间**: 2023年内的随机时间戳

## 性能说明

脚本使用批量插入技术，每批插入1000行数据，以提高插入效率。在插入过程中会显示进度信息。

## 注意事项

1. 运行脚本前请确保PostgreSQL服务正在运行
2. 确保配置的数据库用户具有创建表和插入数据的权限
3. 脚本会删除已存在的同名表，请确保不会影响重要数据
4. 插入10万行数据可能需要一些时间，请耐心等待