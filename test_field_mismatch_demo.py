#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
演示字段不一致时的行为
"""

import tempfile
import os
import sqlite3
from table_diff import TableComparator, SQLiteAdapter


def create_test_database(db_path):
    """创建测试用的数据库和表"""
    conn = sqlite3.connect(db_path)
    
    # 创建两个字段不一致的表
    conn.execute('''
        CREATE TABLE employees_2022 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            department TEXT,
            created_at TEXT
        )
    ''')
    
    conn.execute('''
        CREATE TABLE employees_2023 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            department TEXT,
            phone TEXT,
            created_at TEXT
        )
    ''')
    
    # 插入测试数据
    conn.execute("INSERT INTO employees_2022 (name, email, age, department, created_at) VALUES (?, ?, ?, ?, ?)",
                 ("John Doe", "john@example.com", 30, "IT", "2022-01-01"))
    conn.execute("INSERT INTO employees_2022 (name, email, age, department, created_at) VALUES (?, ?, ?, ?, ?)",
                 ("Jane Smith", "jane@example.com", 25, "HR", "2022-01-02"))
    
    conn.execute("INSERT INTO employees_2023 (name, email, age, department, phone, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                 ("John Doe", "john@example.com", 30, "IT", "1234567890", "2023-01-01"))
    conn.execute("INSERT INTO employees_2023 (name, email, age, department, phone, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                 ("Jane Smith", "jane@example.com", 26, "HR", "0987654321", "2023-01-02"))
    
    conn.commit()
    conn.close()


def main():
    # 创建临时数据库文件
    db_path = tempfile.mktemp(suffix='.db')
    create_test_database(db_path)
    
    try:
        # 创建适配器和比较器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        
        print("=== 测试字段不一致时的行为 ===")
        comparator.set_tables('employees_2022', 'employees_2023')
        result = comparator.compare()
        
        # 显示结果
        print(f"表1行数: {result['table1_row_count']}")
        print(f"表2行数: {result['table2_row_count']}")
        
        if result['differences']:
            print("\n发现差异:")
            for diff in result['differences']:
                print(f"- 类型: {diff['type']}")
                print(f"- 消息: {diff['message']}")
                if 'details' in diff:
                    print(f"- 表1字段: {diff['details']['table1_fields']}")
                    print(f"- 表2字段: {diff['details']['table2_fields']}")
                    print(f"- 仅在表1中: {diff['details']['only_in_table1']}")
                    print(f"- 仅在表2中: {diff['details']['only_in_table2']}")
                    print(f"- 公共字段: {diff['details']['common_fields']}")
        else:
            print("\n没有发现差异")
            
        print("\n=== 使用指定字段进行比较 ===")
        comparator.set_fields(['id', 'name', 'email', 'age', 'department'])
        result = comparator.compare()
        
        # 显示结果
        print(f"表1行数: {result['table1_row_count']}")
        print(f"表2行数: {result['table2_row_count']}")
        
        if result['differences']:
            print("\n发现差异:")
            for diff in result['differences']:
                print(f"- 类型: {diff['type']}")
                print(f"- 消息: {diff['message']}")
        else:
            print("\n没有发现差异")
            if result['row_differences']:
                print("但发现行数据差异:")
                for diff in result['row_differences']:
                    print(f"  行 {diff['row_number']}, 字段 {diff['column_name']}: {diff['table1_value']} != {diff['table2_value']}")
            else:
                print("行数据也一致")
                
    finally:
        # 清理
        adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)


if __name__ == '__main__':
    main()