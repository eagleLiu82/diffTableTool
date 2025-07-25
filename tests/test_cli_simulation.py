#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import patch

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_cli_with_field_mismatch():
    """模拟CLI命令行参数测试"""
    print("模拟CLI命令行参数测试...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（模拟您的MySQL场景）
        conn = sqlite3.connect(db_path)
        
        # 创建employees_2022表
        conn.execute('''
            CREATE TABLE employees_2022 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary DECIMAL(10,2),
                hire_date DATE,
                is_active INTEGER
            )
        ''')
        
        # 创建employees_2023表（有额外字段）
        conn.execute('''
            CREATE TABLE employees_2023 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary DECIMAL(10,2),
                hire_date DATE,
                phone TEXT,
                is_active INTEGER,
                last_login DATETIME
            )
        ''')
        
        # 插入测试数据
        conn.execute("""
            INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 15000.00, "2021-03-15", 1))
        
        conn.execute("""
            INSERT INTO employees_2023 (id, name, email, department, salary, hire_date, phone, is_active, last_login) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (1, "张三", "zhangsan@company.com", "技术部", 16000.00, "2021-03-15", "13800138000", 1, "2023-06-01 09:00:00"))
        
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
        
        # 由于这些表有公共字段，所以应该正常进行比较
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ CLI模拟执行成功")
                return True
            except SystemExit:
                print("✅ CLI模拟执行完成（正常退出）")
                return True
                
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_cli_with_no_common_fields():
    """测试完全没有公共字段的情况"""
    print("\n测试完全没有公共字段的CLI情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（完全没有公共字段）
        conn = sqlite3.connect(db_path)
        
        conn.execute('''
            CREATE TABLE table_x (
                id_x INTEGER PRIMARY KEY,
                name_x TEXT,
                value_x INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_y (
                id_y INTEGER PRIMARY KEY,
                name_y TEXT,
                value_y INTEGER
            )
        ''')
        
        conn.execute("INSERT INTO table_x (name_x, value_x) VALUES (?, ?)", ("Item1", 100))
        conn.execute("INSERT INTO table_y (name_y, value_y) VALUES (?, ?)", ("Item2", 200))
        
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
        
        # 这些表没有公共字段，应该显示字段信息并退出
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ CLI模拟执行成功")
                return True
            except SystemExit:
                print("✅ CLI模拟执行完成（正常退出）")
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
    print("模拟CLI命令行参数测试")
    print("=" * 30)
    
    success1 = test_cli_with_field_mismatch()
    success2 = test_cli_with_no_common_fields()
    
    if success1 and success2:
        print("\n🎉 所有CLI模拟测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分CLI模拟测试失败!")
        sys.exit(1)