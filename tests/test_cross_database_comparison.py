#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sqlite3
import os
import sys
import tempfile
from unittest.mock import Mock, patch, MagicMock

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入主模块
from table_diff import (
    TableComparator, 
    SQLiteAdapter, 
    MySQLAdapter, 
    PostgreSQLAdapter,
    get_database_adapter,
    run_comparison
)


class TestCrossDatabaseComparison(unittest.TestCase):
    """测试跨数据库比较功能"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db1 = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db1.close()
        self.db_path1 = self.temp_db1.name
        
        self.temp_db2 = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db2.close()
        self.db_path2 = self.temp_db2.name
        
        # 创建测试数据库和表
        self._create_test_databases()
        
    def tearDown(self):
        # 清理临时文件
        if os.path.exists(self.db_path1):
            os.unlink(self.db_path1)
        if os.path.exists(self.db_path2):
            os.unlink(self.db_path2)
            
    def _create_test_databases(self):
        """创建测试用的数据库和表"""
        # 创建第一个数据库
        conn1 = sqlite3.connect(self.db_path1)
        
        # 创建测试表
        conn1.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
        
        # 插入测试数据
        conn1.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                     ("Alice", "alice@example.com", 25))
        conn1.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                     ("Bob", "bob@example.com", 30))
        
        conn1.commit()
        conn1.close()
        
        # 创建第二个数据库
        conn2 = sqlite3.connect(self.db_path2)
        
        # 创建测试表（与第一个数据库相同的结构）
        conn2.execute('''
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
        
        # 插入测试数据（有一些差异）
        conn2.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                     ("Alice", "alice@example.com", 25))
        conn2.execute("INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                     ("Bob", "bob@example.com", 31))  # 年龄不同
        
        conn2.commit()
        conn2.close()
        
    def test_table_comparator_with_two_different_adapters(self):
        """测试使用两个不同适配器的TableComparator"""
        print("测试使用两个不同适配器的TableComparator...")
        
        # 创建两个SQLite适配器实例（模拟不同数据库）
        adapter1 = SQLiteAdapter()
        adapter2 = SQLiteAdapter()
        
        # 连接到不同的数据库
        adapter1.connect(db_path=self.db_path1)
        adapter2.connect(db_path=self.db_path2)
        
        # 创建比较器
        comparator = TableComparator(adapter1, adapter2)
        comparator.set_tables('users', 'users')
        
        # 执行比较
        result = comparator.compare()
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertIn('fields', result)
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertEqual(result['table1_row_count'], 2)
        self.assertEqual(result['table2_row_count'], 2)
        
        # 应该有共同字段
        expected_common_fields = ['id', 'name', 'email', 'age']
        self.assertListEqual(sorted(result['fields']), sorted(expected_common_fields))
        
        # 检查是否有差异
        self.assertGreater(len(result['row_differences']), 0)
        
        # 关闭连接
        adapter1.close()
        adapter2.close()
        print("测试使用两个不同适配器的TableComparator完成")
        
    def test_run_comparison_with_different_database_types(self):
        """测试run_comparison函数支持不同数据库类型"""
        print("测试run_comparison函数支持不同数据库类型...")
        
        # 使用run_comparison比较两个SQLite数据库（模拟不同数据库类型）
        result = run_comparison(
            source_db_type='sqlite',
            source_db_path=self.db_path1,
            target_db_type='sqlite',
            target_db_path=self.db_path2,
            table1='users',
            table2='users'
        )
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertIn('fields', result)
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertEqual(result['table1_row_count'], 2)
        self.assertEqual(result['table2_row_count'], 2)
        
        # 应该有共同字段
        expected_common_fields = ['id', 'name', 'email', 'age']
        self.assertListEqual(sorted(result['fields']), sorted(expected_common_fields))
        
        # 检查是否有差异
        self.assertTrue('differences' in result)
        print("测试run_comparison函数支持不同数据库类型完成")
        
    def test_run_comparison_without_target_type_defaults_to_source_type(self):
        """测试run_comparison函数在未指定目标数据库类型时默认使用源数据库类型"""
        print("测试run_comparison函数在未指定目标数据库类型时默认使用源数据库类型...")
        
        # 只提供源数据库类型，不提供目标数据库类型
        result = run_comparison(
            source_db_type='sqlite',
            source_db_path=self.db_path1,
            target_db_path=self.db_path2,
            table1='users',
            table2='users'
        )
        
        # 验证结果
        self.assertIsNotNone(result)
        self.assertIn('fields', result)
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertEqual(result['table1_row_count'], 2)
        self.assertEqual(result['table2_row_count'], 2)
        
        print("测试run_comparison函数在未指定目标数据库类型时默认使用源数据库类型完成")
        
    def test_get_table_fields_from_different_databases(self):
        """测试从不同数据库获取表字段"""
        print("测试从不同数据库获取表字段...")
        
        # 创建两个SQLite适配器实例
        adapter1 = SQLiteAdapter()
        adapter2 = SQLiteAdapter()
        
        # 连接到不同的数据库
        adapter1.connect(db_path=self.db_path1)
        adapter2.connect(db_path=self.db_path2)
        
        # 创建比较器
        comparator = TableComparator(adapter1, adapter2)
        comparator.set_tables('users', 'users')
        
        # 获取表字段
        fields1 = comparator.get_table_fields('users', 1)  # 从第一个数据库获取
        fields2 = comparator.get_table_fields('users', 2)  # 从第二个数据库获取
        
        # 验证字段
        expected_fields = ['id', 'name', 'email', 'age']
        
        self.assertListEqual(sorted(fields1), sorted(expected_fields))
        self.assertListEqual(sorted(fields2), sorted(expected_fields))
        
        # 关闭连接
        adapter1.close()
        adapter2.close()
        print("测试从不同数据库获取表字段完成")
        
    def test_build_query_for_different_databases(self):
        """测试为不同数据库构建查询"""
        print("测试为不同数据库构建查询...")
        
        # 创建两个SQLite适配器实例
        adapter1 = SQLiteAdapter()
        adapter2 = SQLiteAdapter()
        
        # 连接到不同的数据库
        adapter1.connect(db_path=self.db_path1)
        adapter2.connect(db_path=self.db_path2)
        
        # 创建比较器
        comparator = TableComparator(adapter1, adapter2)
        comparator.set_tables('users', 'users')
        
        # 设置比较字段
        comparator.set_fields(['id', 'name', 'email'])
        
        # 为不同数据库构建查询 (注意：由于SQLite有主键，所以会自动添加ORDER BY)
        query1 = comparator.build_query(['id', 'name', 'email'], 'users', 1)
        query2 = comparator.build_query(['id', 'name', 'email'], 'users', 2)
        
        # 验证查询包含基本部分
        expected_query_part = "SELECT id, name, email FROM users"
        self.assertIn(expected_query_part, query1)
        self.assertIn(expected_query_part, query2)
        
        # 关闭连接
        adapter1.close()
        adapter2.close()
        print("测试为不同数据库构建查询完成")


if __name__ == '__main__':
    unittest.main()