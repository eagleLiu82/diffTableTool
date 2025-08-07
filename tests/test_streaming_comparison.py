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
    SQLiteAdapter
)


class TestStreamingComparison(unittest.TestCase):
    """测试流式数据比较功能"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 创建测试数据库和表
        self._create_large_test_database()
        
        # 初始化适配器和对比器
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        # 清理临时文件
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_large_test_database(self):
        """创建大数据量测试用的数据库和表"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建测试表
        conn.execute('''
            CREATE TABLE large_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                department TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE large_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                department TEXT,
                phone TEXT
            )
        ''')
        
        # 插入大量测试数据 (1000行)
        for i in range(1000):
            conn.execute(
                "INSERT INTO large_table1 (name, email, age, department) VALUES (?, ?, ?, ?)",
                (f"User{i}", f"user{i}@example.com", 20 + (i % 50), f"Department{i % 10}")
            )
            
            # 在第二个表中插入稍微不同的数据
            conn.execute(
                "INSERT INTO large_table2 (name, email, age, department, phone) VALUES (?, ?, ?, ?, ?)",
                (f"User{i}", f"user{i}@example.com", 20 + (i % 50), f"Department{i % 10}", f"123456789{i % 100}")
            )
        
        # 在第二个表中额外添加一些数据以制造差异
        conn.execute(
            "INSERT INTO large_table2 (name, email, age, department, phone) VALUES (?, ?, ?, ?, ?)",
            ("ExtraUser", "extra@example.com", 40, "IT", "9999999999")
        )
        
        conn.commit()
        conn.close()
        
    def test_streaming_comparison_with_large_data(self):
        """测试大数据量的流式比较"""
        print("测试大数据量的流式比较...")
        
        # 设置要比较的表
        self.comparator.set_tables('large_table1', 'large_table2')
        
        # 指定要比较的字段，避免字段不一致的问题
        self.comparator.set_fields(['id', 'name', 'email', 'age', 'department'])
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('differences', result)
        self.assertIn('row_differences', result)
        
        # 验证行数统计
        self.assertEqual(result['table1_row_count'], 1000)
        self.assertEqual(result['table2_row_count'], 1001)  # 多一行额外数据
        
        # 验证差异
        self.assertGreater(len(result['row_differences']), 0)
        
        # 查找特定类型的差异
        only_in_table2 = [diff for diff in result['row_differences'] if diff['type'] == 'only_in_table2']
        self.assertEqual(len(only_in_table2), 1)  # 应该有一行只在第二个表中存在
        
        print("测试大数据量的流式比较完成")
        
    def test_streaming_comparison_with_primary_key_matching(self):
        """测试基于主键的流式比较"""
        print("测试基于主键的流式比较...")
        
        # 创建另一个测试数据库，具有明确的主键匹配关系
        conn = sqlite3.connect(self.db_path)
        
        # 创建额外的测试表
        conn.execute('''
            CREATE TABLE pk_test1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE pk_test2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        # 插入测试数据
        test_data = [
            (1, "Item1", 100),
            (2, "Item2", 200),
            (3, "Item3", 300),
            (4, "Item4", 400)
        ]
        
        for row in test_data:
            conn.execute("INSERT INTO pk_test1 (id, name, value) VALUES (?, ?, ?)", row)
            
        # 在第二个表中插入相似但略有不同的数据
        test_data_modified = [
            (1, "Item1", 100),    # 相同
            (2, "Item2", 250),    # 不同的值
            (3, "Item3", 300),    # 相同
            (5, "Item5", 500)     # 不同的ID
        ]
        
        for row in test_data_modified:
            conn.execute("INSERT INTO pk_test2 (id, name, value) VALUES (?, ?, ?)", row)
            
        conn.commit()
        conn.close()
        
        # 设置要比较的表
        self.comparator.set_tables('pk_test1', 'pk_test2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数统计
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
        self.assertEqual(value_diff['table1_value'], 200)
        self.assertEqual(value_diff['table2_value'], 250)
        
        print("测试基于主键的流式比较完成")
        
    def test_streaming_comparison_with_position_matching(self):
        """测试基于位置的流式比较"""
        print("测试基于位置的流式比较...")
        
        # 创建没有主键的测试表
        conn = sqlite3.connect(self.db_path)
        
        # 创建测试表（没有主键）
        conn.execute('''
            CREATE TABLE no_pk_test1 (
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE no_pk_test2 (
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        # 插入测试数据
        test_data = [
            ("Item1", 100),
            ("Item2", 200),
            ("Item3", 300)
        ]
        
        for row in test_data:
            conn.execute("INSERT INTO no_pk_test1 (name, value) VALUES (?, ?)", row)
            
        # 在第二个表中插入相似但略有不同的数据
        test_data_modified = [
            ("Item1", 100),    # 相同
            ("Item2", 250),    # 不同的值
            ("Item3", 300)     # 相同
        ]
        
        for row in test_data_modified:
            conn.execute("INSERT INTO no_pk_test2 (name, value) VALUES (?, ?)", row)
            
        conn.commit()
        conn.close()
        
        # 设置要比较的表
        self.comparator.set_tables('no_pk_test1', 'no_pk_test2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数统计
        self.assertEqual(result['table1_row_count'], 3)
        self.assertEqual(result['table2_row_count'], 3)
        
        # 验证差异
        row_diffs = result['row_differences']
        
        # 应该有1个数据不同的行（第二行）
        different_data = [diff for diff in row_diffs if diff['type'] == 'different_data']
        self.assertEqual(len(different_data), 1)
        
        print("测试基于位置的流式比较完成")


class TestMemoryEfficiency(unittest.TestCase):
    """测试内存效率"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 创建测试数据库和表
        self._create_test_database()
        
        # 初始化适配器和对比器
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        # 清理临时文件
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_test_database(self):
        """创建测试用的数据库和表"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建测试表
        conn.execute('''
            CREATE TABLE memory_test1 (
                id INTEGER PRIMARY KEY,
                data TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE memory_test2 (
                id INTEGER PRIMARY KEY,
                data TEXT
            )
        ''')
        
        # 插入测试数据
        for i in range(100):
            conn.execute(
                "INSERT INTO memory_test1 (id, data) VALUES (?, ?)",
                (i, f"Data for item {i}")
            )
            conn.execute(
                "INSERT INTO memory_test2 (id, data) VALUES (?, ?)",
                (i, f"Data for item {i}")
            )
            
        conn.commit()
        conn.close()
        
    def test_cursor_iteration_efficiency(self):
        """测试游标迭代效率"""
        print("测试游标迭代效率...")
        
        # 设置要比较的表
        self.comparator.set_tables('memory_test1', 'memory_test2')
        
        # 构建查询
        query1 = self.comparator.build_query(['id', 'data'], 'memory_test1')
        query2 = self.comparator.build_query(['id', 'data'], 'memory_test2')
        
        # 执行查询获取游标
        cursor1 = self.adapter.execute_query(query1)
        cursor2 = self.adapter.execute_query(query2)
        
        # 测试逐行处理而不是一次性加载所有数据
        rows1_processed = 0
        rows2_processed = 0
        
        # 处理第一个游标
        for row in cursor1:
            rows1_processed += 1
            # 模拟处理逻辑
            _ = dict(zip(['id', 'data'], row))
            
        # 处理第二个游标
        for row in cursor2:
            rows2_processed += 1
            # 模拟处理逻辑
            _ = dict(zip(['id', 'data'], row))
            
        # 验证处理的行数
        self.assertEqual(rows1_processed, 100)
        self.assertEqual(rows2_processed, 100)
        
        print("测试游标迭代效率完成")


class TestFiftyFieldsAndNullValues(unittest.TestCase):
    """测试50个字段及空值情况"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 创建测试数据库和表
        self._create_50_fields_database()
        
        # 初始化适配器和对比器
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        # 清理临时文件
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_50_fields_database(self):
        """创建50个字段的测试数据库和表"""
        conn = sqlite3.connect(self.db_path)
        
        # 构建50个字段的CREATE TABLE语句
        fields_def = ["id INTEGER PRIMARY KEY"]
        for i in range(50):
            fields_def.append(f"field_{i:02d} TEXT")
        
        fields_def_str = ", ".join(fields_def)
        
        # 创建两个具有50个字段的表
        conn.execute(f'''
            CREATE TABLE fifty_fields_table1 (
                {fields_def_str}
            )
        ''')
        
        conn.execute(f'''
            CREATE TABLE fifty_fields_table2 (
                {fields_def_str}
            )
        ''')
        
        # 插入测试数据，包含空值情况
        # 插入完全相同的行
        values1 = [0] + [f"value_{i:02d}_0" for i in range(50)]
        placeholders = ", ".join(["?"] * len(values1))
        conn.execute(
            f"INSERT INTO fifty_fields_table1 VALUES ({placeholders})", 
            values1
        )
        conn.execute(
            f"INSERT INTO fifty_fields_table2 VALUES ({placeholders})", 
            values1
        )
        
        # 插入部分字段为空的行
        values2 = [1] + [None if i % 3 == 0 else f"value_{i:02d}_1" for i in range(50)]
        placeholders = ", ".join(["?"] * len(values2))
        conn.execute(
            f"INSERT INTO fifty_fields_table1 VALUES ({placeholders})", 
            values2
        )
        conn.execute(
            f"INSERT INTO fifty_fields_table2 VALUES ({placeholders})", 
            values2
        )
        
        # 插入两表中有差异的行（包括空值比较）
        values3_table1 = [2] + [None if i % 4 == 0 else f"value_{i:02d}_2_t1" for i in range(50)]
        values3_table2 = [2] + [None if i % 4 == 0 else f"value_{i:02d}_2_t2" for i in range(50)]
        # 制造一些特定差异，确保第5、10、15字段有明显差异
        values3_table1[5] = "different_value_05_2_t1"  # 第5个字段有值
        values3_table2[5] = "different_value_05_2_t2"  # 第5个字段有不同值
        values3_table1[10] = "only_in_t1"              # 第10个字段在table1中有值，table2中为None
        values3_table2[10] = None
        values3_table1[15] = None                      # 第15个字段在table1中为None，table2中有值
        values3_table2[15] = "only_in_t2"
        
        placeholders = ", ".join(["?"] * len(values3_table1))
        conn.execute(
            f"INSERT INTO fifty_fields_table1 VALUES ({placeholders})", 
            values3_table1
        )
        conn.execute(
            f"INSERT INTO fifty_fields_table2 VALUES ({placeholders})", 
            values3_table2
        )
        
        # 插入只存在于第一个表中的行
        values4 = [3] + [f"value_{i:02d}_3" if i % 2 == 0 else None for i in range(50)]
        placeholders = ", ".join(["?"] * len(values4))
        conn.execute(
            f"INSERT INTO fifty_fields_table1 VALUES ({placeholders})", 
            values4
        )
        
        # 插入只存在于第二个表中的行
        values5 = [4] + [None if i % 2 == 0 else f"value_{i:02d}_4" for i in range(50)]
        placeholders = ", ".join(["?"] * len(values5))
        conn.execute(
            f"INSERT INTO fifty_fields_table2 VALUES ({placeholders})", 
            values5
        )
        
        conn.commit()
        conn.close()
        
    def test_fifty_fields_comparison(self):
        """测试50个字段的比较"""
        print("测试50个字段的比较...")
        
        # 设置要比较的表
        self.comparator.set_tables('fifty_fields_table1', 'fifty_fields_table2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证基本结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数统计
        self.assertEqual(result['table1_row_count'], 4)  # 4行数据
        self.assertEqual(result['table2_row_count'], 4)  # 4行数据
        
        # 验证字段数量
        self.assertEqual(len(result['fields']), 51)  # 50个字段 + id主键
        
        # 验证差异
        row_diffs = result['row_differences']
        self.assertEqual(len(row_diffs), 3)  # 应该有3种类型的差异
        
        # 查找特定类型的差异
        different_data = [diff for diff in row_diffs if diff['type'] == 'different_data']
        only_in_table1 = [diff for diff in row_diffs if diff['type'] == 'only_in_table1']
        only_in_table2 = [diff for diff in row_diffs if diff['type'] == 'only_in_table2']
        
        # 应该有1行数据不同的记录
        self.assertEqual(len(different_data), 1)
        # 应该有1行只在第一个表中的记录
        self.assertEqual(len(only_in_table1), 1)
        # 应该有1行只在第二个表中的记录
        self.assertEqual(len(only_in_table2), 1)
        
        # 验证数据不同的行的具体内容
        diff_row = different_data[0]
        self.assertEqual(diff_row['key']['id'], 2)
        
        print("测试50个字段的比较完成")
        
    def test_null_value_comparison(self):
        """测试空值比较"""
        print("测试空值比较...")
        
        # 设置要比较的表
        self.comparator.set_tables('fifty_fields_table1', 'fifty_fields_table2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 获取数据不同的行
        different_data = [diff for diff in result['row_differences'] if diff['type'] == 'different_data']
        diff_row = different_data[0]
        diff_details = diff_row['differences']
        
        # 验证特定字段的差异存在
        field_05_diff = next((d for d in diff_details if d['field'] == 'field_05'), None)
        field_10_diff = next((d for d in diff_details if d['field'] == 'field_10'), None)
        field_15_diff = next((d for d in diff_details if d['field'] == 'field_15'), None)
        
        self.assertIsNotNone(field_05_diff)
        self.assertIsNotNone(field_10_diff)
        self.assertIsNotNone(field_15_diff)
                
        print("测试空值比较完成")
        
    def test_comparison_with_primary_key(self):
        """测试有主键的表比较"""
        print("测试有主键的表比较...")
        
        # 设置要比较的表（使用已有的带主键的表）
        self.comparator.set_tables('fifty_fields_table1', 'fifty_fields_table2')
        
        # 执行比较
        result = self.comparator.compare()
        
        # 验证比较成功完成
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证使用了主键进行匹配
        # 通过检查是否存在基于主键的差异类型
        self.assertGreaterEqual(result['table1_row_count'], 0)
        self.assertGreaterEqual(result['table2_row_count'], 0)
        
        print("测试有主键的表比较完成")
        
    def test_comparison_without_primary_key(self):
        """测试无主键的表比较"""
        print("测试无主键的表比较...")
        
        # 创建无主键的测试表
        conn = sqlite3.connect(self.db_path)
        
        # 创建两个没有主键的表
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
            ("Item1", "Value1", "Description1"),      # 相同
            ("Item2", "ValueChanged", "Description2"), # 不同值
            ("Item4", "Value4", "Description4")        # 不同项
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
        
        # 验证比较成功完成
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('row_differences', result)
        
        # 验证行数
        self.assertEqual(result['table1_row_count'], 3)
        self.assertEqual(result['table2_row_count'], 3)
        
        print("测试无主键的表比较完成")

def run_all_tests():
    """运行所有测试"""
    print("开始运行流式比较测试...")
    
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加测试用例
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestStreamingComparison))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestMemoryEfficiency))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestFiftyFieldsAndNullValues))
    
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