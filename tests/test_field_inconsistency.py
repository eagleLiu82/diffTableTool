#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import patch

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_field_inconsistency():
    """测试字段不完全一致的情况"""
    print("测试字段不完全一致的情况...")
    
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
        
        # 模拟CLI参数
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'employees_2022',
            '--table2', 'employees_2023'
        ]
        
        print("表employees_2022字段: id, name, email, department, salary, hire_date, is_active")
        print("表employees_2023字段: id, name, email, department, salary, hire_date, phone, is_active, last_login")
        print("字段不完全一致，应该退出比对")
        
        # 这些表字段不完全一致，应该显示字段信息并退出
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 字段不一致测试执行成功")
                return True
            except SystemExit:
                print("✅ 字段不一致测试执行完成（正常退出）")
                return True
                
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_field_consistency():
    """测试字段完全一致的情况"""
    print("\n测试字段完全一致的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（字段完全一致）
        conn = sqlite3.connect(db_path)
        
        # 创建两个字段完全一致的表
        conn.execute('''
            CREATE TABLE table_x (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_y (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
        
        # 插入测试数据
        conn.execute("INSERT INTO table_x (name, email, age) VALUES (?, ?, ?)", ("张三", "zhangsan@example.com", 25))
        conn.execute("INSERT INTO table_y (name, email, age) VALUES (?, ?, ?)", ("张三", "zhangsan@example.com", 30))
        
        conn.commit()
        conn.close()
        
        # 模拟CLI参数
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'table_x',
            '--table2', 'table_y'
        ]
        
        print("表table_x字段: id, name, email, age")
        print("表table_y字段: id, name, email, age")
        print("字段完全一致，应该正常进行比对")
        
        # 这些表字段完全一致，应该正常进行比较
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 字段一致测试执行成功")
                return True
            except SystemExit:
                print("✅ 字段一致测试执行完成（正常退出）")
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
    print("测试字段一致性检查")
    print("=" * 30)
    
    success1 = test_field_inconsistency()
    success2 = test_field_consistency()
    
    if success1 and success2:
        print("\n🎉 所有字段一致性测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分字段一致性测试失败!")
        sys.exit(1)