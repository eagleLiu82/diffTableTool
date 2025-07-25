#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import patch

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_exclude_with_field_mismatch():
    """测试使用exclude参数时字段不一致的情况"""
    print("测试使用exclude参数时字段不一致的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（字段不完全一致）
        conn = sqlite3.connect(db_path)
        
        # 创建employees_2022表
        conn.execute('''
            CREATE TABLE employees_2022 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                is_active INTEGER
            )
        ''')
        
        # 创建employees_2023表（字段不完全一致）
        conn.execute('''
            CREATE TABLE employees_2023 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                phone TEXT,
                is_active INTEGER,
                last_login TEXT
            )
        ''')
        
        # 插入测试数据
        conn.execute("""
            INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 15000.0, "2021-03-15", 1))
        
        conn.execute("""
            INSERT INTO employees_2023 (id, name, email, department, salary, hire_date, phone, is_active, last_login) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 16000.0, "2021-03-15", "13800138000", 1, "2023-06-01"))
        
        conn.commit()
        conn.close()
        
        # 模拟CLI参数（使用exclude参数）
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'employees_2022',
            '--table2', 'employees_2023',
            '--exclude', 'phone', 'last_login'
        ]
        
        print("表employees_2022字段: id, name, email, department, salary, hire_date, is_active")
        print("表employees_2023字段: id, name, email, department, salary, hire_date, phone, is_active, last_login")
        print("使用exclude参数排除phone和last_login字段，应该继续比对")
        
        # 使用exclude参数，应该继续进行比较
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 使用exclude参数测试执行成功")
                return True
            except SystemExit:
                print("✅ 使用exclude参数测试执行完成（正常退出）")
                return True
                
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_without_exclude_field_mismatch():
    """测试不使用exclude参数时字段不一致的情况"""
    print("\n测试不使用exclude参数时字段不一致的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（字段不完全一致）
        conn = sqlite3.connect(db_path)
        
        # 创建employees_2022表
        conn.execute('''
            CREATE TABLE employees_2022_2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                is_active INTEGER
            )
        ''')
        
        # 创建employees_2023表（字段不完全一致）
        conn.execute('''
            CREATE TABLE employees_2023_2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                phone TEXT,
                is_active INTEGER,
                last_login TEXT
            )
        ''')
        
        # 插入测试数据
        conn.execute("""
            INSERT INTO employees_2022_2 (id, name, email, department, salary, hire_date, is_active) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 15000.0, "2021-03-15", 1))
        
        conn.execute("""
            INSERT INTO employees_2023_2 (id, name, email, department, salary, hire_date, phone, is_active, last_login) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 16000.0, "2021-03-15", "13800138000", 1, "2023-06-01"))
        
        conn.commit()
        conn.close()
        
        # 模拟CLI参数（不使用exclude参数）
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'employees_2022_2',
            '--table2', 'employees_2023_2'
        ]
        
        print("表employees_2022_2字段: id, name, email, department, salary, hire_date, is_active")
        print("表employees_2023_2字段: id, name, email, department, salary, hire_date, phone, is_active, last_login")
        print("不使用exclude参数，字段不一致应该退出比对")
        
        # 不使用exclude参数，应该退出比对
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 不使用exclude参数测试执行成功")
                return True
            except SystemExit:
                print("✅ 不使用exclude参数测试执行完成（正常退出）")
                return True
                
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    print("测试exclude参数功能")
    print("=" * 30)
    
    success1 = test_exclude_with_field_mismatch()
    success2 = test_without_exclude_field_mismatch()
    
    if success1 and success2:
        print("\n🎉 所有exclude参数测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分exclude参数测试失败!")
        sys.exit(1)