-- MySQL测试数据脚本

-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS test_db;
USE test_db;

-- 删除已存在的表
DROP TABLE IF EXISTS employees_2022;
DROP TABLE IF EXISTS employees_2023;

-- 创建2022年员工表
CREATE TABLE employees_2022 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10, 2),
    hire_date DATE,
    is_active TINYINT(1)
);

-- 创建2023年员工表（结构略有不同）
CREATE TABLE employees_2023 (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10, 2),
    hire_date DATE,
    phone VARCHAR(20),
    is_active TINYINT(1),
    last_login DATETIME
);

-- 插入2022年员工数据
INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active) VALUES
(1, '张三', 'zhangsan@company.com', '技术部', 15000.00, '2021-03-15', 1),
(2, '李四', 'lisi@company.com', '销售部', 12000.00, '2020-07-01', 1),
(3, '王五', 'wangwu@company.com', '人事部', 10000.00, '2022-01-10', 0);

-- 插入2023年员工数据（有变化）
INSERT INTO employees_2023 (id, name, email, department, salary, hire_date, phone, is_active, last_login) VALUES
(1, '张三', 'zhangsan@company.com', '技术部', 16000.00, '2021-03-15', '13800138000', 1, '2023-06-01 09:00:00'),
(2, '李四', 'lisi@company.com', '销售部', 13000.00, '2020-07-01', '13800138001', 1, '2023-06-02 10:30:00'),
(3, '王五', 'wangwu@company.com', '人事部', 10000.00, '2022-01-10', '13800138002', 0, NULL),
(4, '赵六', 'zhaoliu@company.com', '市场部', 11000.00, '2023-05-01', '13800138003', 1, '2023-06-03 14:15:00');

-- 查询验证数据
SELECT 'employees_2022表数据:' AS info;
SELECT * FROM employees_2022;

SELECT 'employees_2023表数据:' AS info;
SELECT * FROM employees_2023;