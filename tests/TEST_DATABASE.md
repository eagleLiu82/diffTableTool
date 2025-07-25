# 测试数据库说明

本文档描述了用于测试数据库表对比工具的示例数据库结构和数据。

## 数据库结构

我们创建了两个表来模拟不同时间点的员工数据：

- `employees_2022`: 2022年的员工数据
- `employees_2023`: 2023年的员工数据（结构略有变化）

### 表结构差异

1. **employees_2022** 字段：
   - `id`: 员工ID（主键）
   - `name`: 员工姓名
   - `email`: 邮箱地址
   - `department`: 部门
   - `salary`: 薪资
   - `hire_date`: 入职日期
   - `is_active`: 是否在职状态

2. **employees_2023** 字段：
   - 包含employees_2022的所有字段
   - 新增字段：
     - `phone`: 电话号码
     - `last_login`: 最后登录时间

## 测试数据

### employees_2022 表数据

| id | name | email | department | salary | hire_date | is_active |
|----|------|-------|------------|--------|-----------|-----------|
| 1 | 张三 | zhangsan@company.com | 技术部 | 15000.00 | 2021-03-15 | 1 |
| 2 | 李四 | lisi@company.com | 销售部 | 12000.00 | 2020-07-01 | 1 |
| 3 | 王五 | wangwu@company.com | 人事部 | 10000.00 | 2022-01-10 | 0 |

### employees_2023 表数据

| id | name | email | department | salary | hire_date | phone | is_active | last_login |
|----|------|-------|------------|--------|-----------|-------|-----------|------------|
| 1 | 张三 | zhangsan@company.com | 技术部 | 16000.00 | 2021-03-15 | 13800138000 | 1 | 2023-06-01 09:00:00 |
| 2 | 李四 | lisi@company.com | 销售部 | 13000.00 | 2020-07-01 | 13800138001 | 1 | 2023-06-02 10:30:00 |
| 3 | 王五 | wangwu@company.com | 人事部 | 10000.00 | 2022-01-10 | 13800138002 | 0 | NULL |
| 4 | 赵六 | zhaoliu@company.com | 市场部 | 11000.00 | 2023-05-01 | 13800138003 | 1 | 2023-06-03 14:15:00 |

## 预期的对比结果

使用数据库表对比工具对比这两个表时，应得到以下结果：

1. **公共字段**: id, name, email, department, salary, hire_date, is_active
2. **行数差异**: 
   - employees_2022: 3行
   - employees_2023: 4行
3. **数据差异**:
   - 员工"张三"薪资从15000变为16000
   - 员工"李四"薪资从12000变为13000
   - 员工"王五"数据无变化
   - employees_2023新增员工"赵六"

## 不同数据库的实现

我们为不同的数据库系统提供了相应的SQL脚本：

1. **MySQL**: [mysql_test_data.sql](mysql_test_data.sql)
2. **PostgreSQL**: [postgresql_test_data.sql](postgresql_test_data.sql)

这些脚本可以用于在相应的数据库系统中创建测试数据，以验证工具对不同数据库的支持能力。