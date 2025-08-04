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
from table_diff_gui import QueryComparator
from table_diff import SQLiteAdapter


class TestQueryComparator(unittest.TestCase):
    """测试查询对比功能"""

    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # 创建测试数据库和表
        self._create_test_database()

        # 创建数据库适配器
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)

    def tearDown(self):
        # 关闭数据库连接
        self.adapter.close()
        # 清理临时文件
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def _create_test_database(self):
        """创建测试用的数据库和表"""
        conn = sqlite3.connect(self.db_path)

        # 创建测试表
        conn.execute('''
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT,
                salary INTEGER
            )
        ''')

        conn.execute('''
            CREATE TABLE departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                budget INTEGER
            )
        ''')

        # 插入测试数据
        conn.execute("INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                     ("Alice", "IT", 5000))
        conn.execute("INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                     ("Bob", "HR", 4500))
        conn.execute("INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                     ("Charlie", "IT", 5500))

        conn.execute("INSERT INTO departments (name, budget) VALUES (?, ?)",
                     ("IT", 100000))
        conn.execute("INSERT INTO departments (name, budget) VALUES (?, ?)",
                     ("HR", 80000))

        conn.commit()
        conn.close()

    def test_compare_queries_same_structure(self):
        """测试相同结构的查询对比"""
        print("测试相同结构的查询对比...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个相似的查询
        query1 = "SELECT name, department FROM employees WHERE department = 'IT'"
        query2 = "SELECT name, department FROM employees WHERE department = 'IT'"

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertEqual(len(result['row_differences']), 0)  # 应该没有差异
        self.assertEqual(result['table1_row_count'], result['table2_row_count'])
        self.assertEqual(result['table1_row_count'], 2)  # IT部门有2个员工

        print("测试相同结构的查询对比完成")

    def test_compare_queries_different_data(self):
        """测试不同数据的查询对比"""
        print("测试不同数据的查询对比...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个不同的查询
        query1 = "SELECT name, salary FROM employees WHERE department = 'IT' ORDER BY name"
        query2 = "SELECT name, salary FROM employees WHERE department = 'HR' ORDER BY name"

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertGreater(len(result['row_differences']), 0)  # 应该有差异
        self.assertEqual(result['table1_row_count'], 2)  # IT部门有2个员工
        self.assertEqual(result['table2_row_count'], 1)  # HR部门有1个员工

        print("测试不同数据的查询对比完成")

    def test_compare_queries_different_columns(self):
        """测试不同列结构的查询对比"""
        print("测试不同列结构的查询对比...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个不同列结构的查询
        query1 = "SELECT name, department FROM employees"
        query2 = "SELECT name, budget FROM departments"

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertEqual(len(result['differences']), 1)  # 应该有一个字段不匹配的差异
        self.assertEqual(result['differences'][0]['type'], 'field_mismatch')
        self.assertIn('department', result['table1_fields'])
        self.assertIn('budget', result['table2_fields'])

        print("测试不同列结构的查询对比完成")

    def test_compare_queries_different_row_counts(self):
        """测试不同行数的查询对比"""
        print("测试不同行数的查询对比...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个返回不同行数的查询
        query1 = "SELECT name FROM employees"  # 应该返回3行
        query2 = "SELECT name FROM employees WHERE department = 'HR'"  # 应该返回1行

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertEqual(result['table1_row_count'], 3)
        self.assertEqual(result['table2_row_count'], 1)

        # 应该有行数差异的标识
        diff_types = [diff['type'] for diff in result['differences']]
        self.assertIn('multiple_row_diff', diff_types)

        print("测试不同行数的查询对比完成")

    def test_empty_query_results(self):
        """测试空查询结果的对比"""
        print("测试空查询结果的对比...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个都返回空结果的查询
        query1 = "SELECT name FROM employees WHERE department = 'NONEXISTENT'"
        query2 = "SELECT name FROM employees WHERE department = 'NONEXISTENT'"

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertEqual(result['table1_row_count'], 0)
        self.assertEqual(result['table2_row_count'], 0)
        self.assertEqual(len(result['row_differences']), 0)  # 应该没有差异

        print("测试空查询结果的对比完成")

    def test_single_row_difference(self):
        """测试单行数据差异"""
        print("测试单行数据差异...")

        # 创建查询对比器
        comparator = QueryComparator(self.adapter)

        # 定义两个只有部分数据不同的查询
        query1 = "SELECT name, salary FROM employees WHERE name = 'Alice'"
        query2 = "SELECT name, salary FROM employees WHERE name = 'Bob'"

        # 执行对比
        result = comparator.compare_queries(query1, query2)

        # 验证结果
        self.assertEqual(result['table1_row_count'], 1)
        self.assertEqual(result['table2_row_count'], 1)
        self.assertEqual(len(result['row_differences']), 1)  # 应该有1行差异

        # 检查具体差异
        diff_row = result['row_differences'][0]
        self.assertEqual(diff_row['type'], 'different_data')
        self.assertEqual(diff_row['row_number'], 1)

        # 检查字段差异
        diffs = diff_row['differences']
        self.assertGreater(len(diffs), 0)

        # 查找name字段的差异
        name_diff = next((d for d in diffs if d['field'] == 'name'), None)
        self.assertIsNotNone(name_diff)
        self.assertEqual(name_diff['table1_value'], 'Alice')
        self.assertEqual(name_diff['table2_value'], 'Bob')

        print("测试单行数据差异完成")


if __name__ == '__main__':
    unittest.main()