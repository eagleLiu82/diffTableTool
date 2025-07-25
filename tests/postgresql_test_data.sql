-- PostgreSQL测试数据脚本

-- 创建测试数据库（需要在psql命令行中使用）
-- CREATE DATABASE test_db;

-- 连接到test_db数据库
-- \c test_db;

-- 删除已存在的表
DROP TABLE IF EXISTS employees_2022;
DROP TABLE IF EXISTS employees_2023;

-- 创建2022年员工表
CREATE TABLE employees_2022 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary NUMERIC(10, 2),
    hire_date DATE,
    is_active BOOLEAN
);

-- 创建2023年员工表（结构略有不同）
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

-- 插入2022年员工数据
INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active) VALUES
(1, '张三', 'zhangsan@company.com', '技术部', 15000.00, '2021-03-15', true),
(2, '李四', 'lisi@company.com', '销售部', 12000.00, '2020-07-01', true),
(3, '王五', 'wangwu@company.com', '人事部', 10000.00, '2022-01-10', false);

-- 插入2023年员工数据（有变化）
INSERT INTO employees_2023 (id, name, email, department, salary, hire_date, phone, is_active, last_login) VALUES
(1, '张三', 'zhangsan@company.com', '技术部', 16000.00, '2021-03-15', '13800138000', true, '2023-06-01 09:00:00'),
(2, '李四', 'lisi@company.com', '销售部', 13000.00, '2020-07-01', '13800138001', true, '2023-06-02 10:30:00'),
(3, '王五', 'wangwu@company.com', '人事部', 10000.00, '2022-01-10', '13800138002', false, NULL),
(4, '赵六', 'zhaoliu@company.com', '市场部', 11000.00, '2023-05-01', '13800138003', true, '2023-06-03 14:15:00');

-- 查询验证数据
SELECT 'employees_2022表数据:' AS info;
SELECT * FROM employees_2022;

SELECT 'employees_2023表数据:' AS info;
SELECT * FROM employees_2023;