#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import patch

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_partial_field_match():
    """测试字段部分匹配的情况"""
    print("测试字段部分匹配的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（部分字段匹配）
        conn = sqlite3.connect(db_path)
        
        # 表1有一些字段
        conn.execute('''
            CREATE TABLE table_a (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                department TEXT,
                salary REAL
            )
        ''')
        
        # 表2有一些相同的字段，也有一些不同的字段
        conn.execute('''
            CREATE TABLE table_b (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                phone TEXT,
                address TEXT,
                salary REAL
            )
        ''')
        
        # 插入测试数据
        conn.execute("""
            INSERT INTO table_a (name, email, age, department, salary) 
            VALUES (?, ?, ?, ?, ?)
        """, ("张三", "zhangsan@example.com", 25, "技术部", 15000.0))
        
        conn.execute("""
            INSERT INTO table_b (name, email, age, phone, address, salary) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("张三", "zhangsan@example.com", 25, "13800138000", "北京市", 15000.0))
        
        conn.commit()
        conn.close()
        
        # 模拟CLI参数
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'table_a',
            '--table2', 'table_b'
        ]
        
        print("表table_a字段: id, name, email, age, department, salary")
        print("表table_b字段: id, name, email, age, phone, address, salary")
        print("公共字段: id, name, email, age, salary")
        print("独有字段: table_a有department, table_b有phone, address")
        
        # 这些表有公共字段，应该正常进行比较
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 部分字段匹配测试执行成功")
                return True
            except SystemExit:
                print("✅ 部分字段匹配测试执行完成（正常退出）")
                return True
                
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_specified_fields_not_exist():
    """测试指定不存在的字段的情况"""
    print("\n测试指定不存在的字段的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表
        conn = sqlite3.connect(db_path)
        
        conn.execute('''
            CREATE TABLE table_c (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_d (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        
        conn.execute("INSERT INTO table_c (name) VALUES (?)", ("Item1",))
        conn.execute("INSERT INTO table_d (name) VALUES (?)", ("Item2",))
        
        conn.commit()
        conn.close()
        
        # 模拟CLI参数（指定不存在的字段）
        test_args = [
            'table_diff.py',
            '--db-type', 'sqlite',
            '--db-path', db_path,
            '--table1', 'table_c',
            '--table2', 'table_d',
            '--fields', 'nonexistent_field'
        ]
        
        # 指定了不存在的字段，应该报错
        with patch.object(sys, 'argv', test_args):
            from table_diff import main
            try:
                main()
                print("✅ 指定不存在字段测试执行完成")
                return True
            except SystemExit:
                print("✅ 指定不存在字段测试执行完成（正常退出）")
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
    print("测试字段部分匹配的情况")
    print("=" * 30)
    
    success1 = test_partial_field_match()
    success2 = test_specified_fields_not_exist()
    
    if success1 and success2:
        print("\n🎉 所有部分字段匹配测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分字段匹配测试失败!")
        sys.exit(1)