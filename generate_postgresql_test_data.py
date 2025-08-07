#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import random
from datetime import datetime, date, timedelta
import sys
import os

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入PostgreSQL配置
try:
    from postgresql_config import POSTGRESQL_CONFIG
except ImportError:
    print("未找到postgresql_config.py配置文件，请创建该文件并配置数据库连接参数")
    sys.exit(1)

# 用于生成测试数据的辅助函数
def generate_random_name():
    """生成随机姓名"""
    first_names = ['张', '李', '王', '刘', '陈', '杨', '赵', '黄', '周', '吴',
                   '徐', '孙', '胡', '朱', '高', '林', '何', '郭', '马', '罗']
    last_names = ['伟', '芳', '娜', '敏', '静', '丽', '强', '磊', '军', '洋',
                  '勇', '艳', '杰', '娟', '涛', '明', '超', '秀英', '霞', '平']
    return random.choice(first_names) + random.choice(last_names)

def generate_random_email(name, index):
    """生成随机邮箱"""
    domains = ['gmail.com', 'yahoo.com', 'hotmail.com', '163.com', 'qq.com', 'sina.com']
    return f"{name}{index}@{random.choice(domains)}"

def generate_random_department():
    """生成随机部门"""
    departments = ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Operations', 'R&D', 'Support']
    return random.choice(departments)

def generate_random_salary():
    """生成随机薪资"""
    return round(random.uniform(30000, 150000), 2)

def generate_random_hire_date():
    """生成随机入职日期"""
    start_date = date(2020, 1, 1)
    end_date = date(2023, 12, 31)
    time_between = end_date - start_date
    days_between = time_between.days
    random_number_of_days = random.randrange(days_between)
    return start_date + timedelta(days=random_number_of_days)

def generate_random_phone():
    """生成随机电话号码"""
    return f"1{random.randint(3, 9)}{random.randint(0, 9)}{random.randint(10000000, 99999999)}"

def generate_random_timestamp():
    """生成随机时间戳"""
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    time_between = end_date - start_date
    seconds_between = int(time_between.total_seconds())
    random_seconds = random.randint(0, seconds_between)
    return start_date + timedelta(seconds=random_seconds)

def create_tables(cursor):
    """创建测试表"""
    print("创建测试表...")
    
    # 删除已存在的表
    try:
        cursor.execute("DROP TABLE IF EXISTS employees_2022")
        cursor.execute("DROP TABLE IF EXISTS employees_2023")
    except:
        pass
    
    # 创建2022年员工表
    cursor.execute("""
        CREATE TABLE employees_2022 (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100),
            department VARCHAR(50),
            salary NUMERIC(10, 2),
            hire_date DATE,
            is_active BOOLEAN
        )
    """)
    
    # 创建2023年员工表（结构略有不同）
    cursor.execute("""
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
        )
    """)
    
    print("测试表创建完成")

def insert_employees_2022(cursor, connection, count=100000):
    """向employees_2022表插入数据"""
    print(f"开始向employees_2022表插入{count}行数据...")
    
    batch_size = 1000
    inserted_count = 0
    
    for batch in range(0, count, batch_size):
        batch_data = []
        current_batch_size = min(batch_size, count - batch)
        
        for i in range(current_batch_size):
            name = generate_random_name()
            email = generate_random_email(name, batch + i)
            department = generate_random_department()
            salary = generate_random_salary()
            hire_date = generate_random_hire_date()
            is_active = random.choice([True, False])
            
            batch_data.append((name, email, department, salary, hire_date, is_active))
        
        # 批量插入数据
        cursor.executemany("""
            INSERT INTO employees_2022 (name, email, department, salary, hire_date, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, batch_data)
        
        connection.commit()
        inserted_count += current_batch_size
        print(f"已插入 {inserted_count}/{count} 行数据到employees_2022表")
    
    print(f"成功向employees_2022表插入{inserted_count}行数据")

def insert_employees_2023(cursor, connection, count=100000):
    """向employees_2023表插入数据"""
    print(f"开始向employees_2023表插入{count}行数据...")
    
    batch_size = 1000
    inserted_count = 0
    
    for batch in range(0, count, batch_size):
        batch_data = []
        current_batch_size = min(batch_size, count - batch)
        
        for i in range(current_batch_size):
            name = generate_random_name()
            email = generate_random_email(name, batch + i)
            department = generate_random_department()
            salary = generate_random_salary()
            hire_date = generate_random_hire_date()
            phone = generate_random_phone()
            is_active = random.choice([True, False])
            last_login = generate_random_timestamp()
            
            batch_data.append((name, email, department, salary, hire_date, phone, is_active, last_login))
        
        # 批量插入数据
        cursor.executemany("""
            INSERT INTO employees_2023 (name, email, department, salary, hire_date, phone, is_active, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, batch_data)
        
        connection.commit()
        inserted_count += current_batch_size
        print(f"已插入 {inserted_count}/{count} 行数据到employees_2023表")
    
    print(f"成功向employees_2023表插入{inserted_count}行数据")

def main():
    """主函数"""
    try:
        # 尝试连接到PostgreSQL数据库
        print("连接到PostgreSQL数据库...")
        connection = psycopg2.connect(
            host=POSTGRESQL_CONFIG['host'],
            port=POSTGRESQL_CONFIG['port'],
            user=POSTGRESQL_CONFIG['user'],
            password=POSTGRESQL_CONFIG['password'],
            database=POSTGRESQL_CONFIG['database']
        )
        cursor = connection.cursor()
        print("数据库连接成功")
        
        # 创建表
        create_tables(cursor)
        
        # 插入数据
        insert_employees_2022(cursor, connection, 100000)
        insert_employees_2023(cursor, connection, 100000)
        
        # 关闭连接
        cursor.close()
        connection.close()
        print("所有数据插入完成，数据库连接已关闭")
        
    except Exception as e:
        print(f"PostgreSQL错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()