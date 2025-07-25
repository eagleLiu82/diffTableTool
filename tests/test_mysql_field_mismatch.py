#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import Mock

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import TableComparator, SQLiteAdapter

def test_field_mismatch_like_mysql():
    """模拟MySQL环境下字段不一致的情况"""
    print("模拟MySQL环境下字段不一致的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（模拟MySQL中的employees_2022和employees_2023表）
        conn = sqlite3.connect(db_path)
        
        # 创建employees_2022表（模拟MySQL表结构）
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
        
        # 创建employees_2023表（字段略有不同）
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
        
        # 注意：这些表实际上是有公共字段的，所以不会触发"没有公共字段"的逻辑
        # 这里是为了测试正常流程
        
        # 测试表对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('employees_2022', 'employees_2023')
        
        # 执行对比
        result = comparator.compare()
        
        # 验证结果
        print(f"字段列表: {', '.join(result['fields'])}")
        print(f"表employees_2022行数: {result['table1_row_count']}")
        print(f"表employees_2023行数: {result['table2_row_count']}")
        
        # 应该有公共字段
        expected_common_fields = ['id', 'name', 'email', 'department', 'salary', 'hire_date', 'is_active']
        actual_common_fields = sorted(result['fields'])
        expected_common_fields = sorted(expected_common_fields)
        
        if actual_common_fields == expected_common_fields:
            print("✅ 正确识别了公共字段")
        else:
            print(f"❌ 公共字段识别错误。期望: {expected_common_fields}, 实际: {actual_common_fields}")
            return False
            
        # 清理
        adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
            
        print("✅ MySQL环境模拟测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        # 清理
        if 'adapter' in locals():
            adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False

def test_no_common_fields_case():
    """测试完全没有公共字段的情况"""
    print("\n测试完全没有公共字段的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（完全没有公共字段）
        conn = sqlite3.connect(db_path)
        
        # 创建两个完全没有公共字段的表
        conn.execute('''
            CREATE TABLE table_a (
                id_a INTEGER PRIMARY KEY,
                name_a TEXT,
                value_a INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_b (
                id_b INTEGER PRIMARY KEY,
                name_b TEXT,
                value_b INTEGER
            )
        ''')
        
        conn.execute("INSERT INTO table_a (name_a, value_a) VALUES (?, ?)", ("Item1", 100))
        conn.execute("INSERT INTO table_b (name_b, value_b) VALUES (?, ?)", ("Item2", 200))
        
        conn.commit()
        conn.close()
        
        # 测试表对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('table_a', 'table_b')
        
        # 执行对比
        result = comparator.compare()
        
        # 验证结果
        print(f"字段列表: {result['fields']}")
        print(f"差异信息: {result['differences']}")
        print(f"表table_a字段: {result['table1_fields']}")
        print(f"表table_b字段: {result['table2_fields']}")
        
        # 验证是否正确处理了没有公共字段的情况
        if (len(result['fields']) == 0 and 
            len(result['differences']) == 1 and 
            result['differences'][0]['type'] == 'no_common_fields'):
            print("✅ 正确处理了没有公共字段的情况")
        else:
            print("❌ 没有正确处理没有公共字段的情况")
            return False
            
        # 清理
        adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
            
        print("✅ 无公共字段测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        # 清理
        if 'adapter' in locals():
            adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)
        return False

if __name__ == "__main__":
    print("测试MySQL环境下字段不一致的处理")
    print("=" * 40)
    
    success1 = test_field_mismatch_like_mysql()
    success2 = test_no_common_fields_case()
    
    if success1 and success2:
        print("\n🎉 所有测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败!")
        sys.exit(1)