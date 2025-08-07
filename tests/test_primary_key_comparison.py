#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sqlite3
import os
import sys
import tempfile

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入主模块
from table_diff import (
    TableComparator, 
    SQLiteAdapter
)


class TestPrimaryKeyComparison(unittest.TestCase):
    """测试基于主键和无主键的表比较"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 初始化适配器和对比器
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        # 清理临时文件
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def test_comparison_with_primary_key(self):
        """测试有主键的表比较"""
        print("测试有主键的表比较...")
        
        # 创建带主键的测试表
        conn = sqlite3.connect(self.db_path)
        
        conn.execute('''
            CREATE TABLE pk_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE pk_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT
            )
        ''')
        
        # 插入测试数据到第一个表
        test_data1 = [
            (1, "Item1", "Value1"),
            (2, "Item2", "Value2"),
            (3, "Item3", "Value3"),
            (4, "Item4", "Value4")
        ]
        
        # 插入测试数据到第二个表
        test_data2 = [
            (1, "Item1", "Value1"),
            (2, "Item2", "ValueChanged"),  # 不同值
            (3, "Item3", "Value3"),
            (5, "Item5", "Value5")         # 不同ID
        ]
        
        for row in test_data1:
            conn.execute("INSERT INTO pk_table1 VALUES (?, ?, ?)", row)
            
        for row in test_data2:
            conn.execute("INSERT INTO pk_table2 VALUES (?, ?, ?)", row)
            
        conn.commit()
        conn.close()
        
        # 设置要比较的表
        self.comparator.set_tables('pk_table1', 'pk_table2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数
        self.assertEqual(result['table1_row_count'], 4)
        self.assertEqual(result['table2_row_count'], 4)
        
        # 验证差异类型
        row_diffs = result['row_differences']
        
        # 应该有三种类型的差异:
        # 1. 数据不同的行 (id=2)
        # 2. 只在第一个表中的行 (id=4)
        # 3. 只在第二个表中的行 (id=5)
        different_data = [diff for diff in row_diffs if diff['type'] == 'different_data']
        only_in_table1 = [diff for diff in row_diffs if diff['type'] == 'only_in_table1']
        only_in_table2 = [diff for diff in row_diffs if diff['type'] == 'only_in_table2']
        
        self.assertEqual(len(different_data), 1)
        self.assertEqual(len(only_in_table1), 1)
        self.assertEqual(len(only_in_table2), 1)
        
        # 验证具体差异内容
        # 检查数据不同的行
        diff_row = different_data[0]
        self.assertEqual(diff_row['key']['id'], 2)
        diff_details = diff_row['differences']
        value_diff = [d for d in diff_details if d['field'] == 'value'][0]
        self.assertEqual(value_diff['table1_value'], 'Value2')
        self.assertEqual(value_diff['table2_value'], 'ValueChanged')
        
        print("测试有主键的表比较完成")
        
    def test_comparison_without_primary_key(self):
        """测试无主键的表比较"""
        print("测试无主键的表比较...")
        
        # 创建无主键的测试表
        conn = sqlite3.connect(self.db_path)
        
        conn.execute('''
            CREATE TABLE no_pk_table1 (
                name TEXT,
                value TEXT,
                description TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE no_pk_table2 (
                name TEXT,
                value TEXT,
                description TEXT
            )
        ''')
        
        # 插入测试数据
        test_data1 = [
            ("Item1", "Value1", "Description1"),
            ("Item2", "Value2", "Description2"),
            ("Item3", "Value3", "Description3")
        ]
        
        test_data2 = [
            ("Item1", "Value1", "Description1"),       # 相同
            ("Item2", "ValueChanged", "Description2"), # 不同值
            ("Item3", "Value3", "Description3")        # 相同（修复之前的问题）
        ]
        
        for row in test_data1:
            conn.execute("INSERT INTO no_pk_table1 VALUES (?, ?, ?)", row)
            
        for row in test_data2:
            conn.execute("INSERT INTO no_pk_table2 VALUES (?, ?, ?)", row)
            
        conn.commit()
        conn.close()
        
        # 设置要比较的表
        self.comparator.set_tables('no_pk_table1', 'no_pk_table2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数
        self.assertEqual(result['table1_row_count'], 3)
        self.assertEqual(result['table2_row_count'], 3)
        
        # 验证差异
        row_diffs = result['row_differences']
        
        # 应该有1个数据不同的行（第二行）
        different_data = [diff for diff in row_diffs if diff['type'] == 'different_data']
        self.assertEqual(len(different_data), 1)
        
        print("测试无主键的表比较完成")


def run_all_tests():
    """运行所有测试"""
    print("开始运行主键比较测试...")
    
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加测试用例
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestPrimaryKeyComparison))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print(f"\n测试完成!")
    print(f"运行测试数: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_all_tests()
    exit(0 if success else 1)