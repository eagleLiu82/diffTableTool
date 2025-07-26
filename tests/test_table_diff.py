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
    create_sample_database
)


class TestDatabaseAdapters(unittest.TestCase):
    """测试数据库适配器类"""
    
    def setUp(self):
        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # 创建测试数据库和表
        self._create_test_database()
        
    def tearDown(self):
        # 清理临时文件
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_test_database(self):
        """创建测试用的数据库和表"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建测试表
        conn.execute('''
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE test_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                phone TEXT
            )
        ''')
        
        # 插入测试数据
        conn.execute("INSERT INTO test_table1 (name, email, age) VALUES (?, ?, ?)",
                     ("Alice", "alice@example.com", 25))
        conn.execute("INSERT INTO test_table1 (name, email, age) VALUES (?, ?, ?)",
                     ("Bob", "bob@example.com", 30))
        
        conn.execute("INSERT INTO test_table2 (name, email, age, phone) VALUES (?, ?, ?, ?)",
                     ("Alice", "alice@example.com", 25, "123456789"))
        conn.execute("INSERT INTO test_table2 (name, email, age, phone) VALUES (?, ?, ?, ?)",
                     ("Bob", "bob@example.com", 31, "987654321"))  # 年龄不同
        
        conn.commit()
        conn.close()
        
    def test_sqlite_adapter_connection(self):
        """测试SQLite适配器连接"""
        print("测试SQLite适配器连接...")
        adapter = SQLiteAdapter()
        connection = adapter.connect(db_path=self.db_path)
        self.assertIsNotNone(connection)
        adapter.close()
        print("测试SQLite适配器连接完成")
        
    def test_sqlite_adapter_get_table_fields(self):
        """测试SQLite适配器获取表字段"""
        print("测试SQLite适配器获取表字段...")
        adapter = SQLiteAdapter()
        adapter.connect(db_path=self.db_path)
        
        fields1 = adapter.get_table_fields('test_table1')
        fields2 = adapter.get_table_fields('test_table2')
        
        self.assertIn('id', fields1)
        self.assertIn('name', fields1)
        self.assertIn('email', fields1)
        self.assertIn('age', fields1)
        
        self.assertIn('phone', fields2)
        adapter.close()
        print("测试SQLite适配器获取表字段完成")
        
    def test_sqlite_adapter_execute_query(self):
        """测试SQLite适配器执行查询"""
        print("测试SQLite适配器执行查询...")
        adapter = SQLiteAdapter()
        adapter.connect(db_path=self.db_path)
        
        cursor = adapter.execute_query("SELECT * FROM test_table1")
        rows = cursor.fetchall()
        
        self.assertEqual(len(rows), 2)
        adapter.close()
        print("测试SQLite适配器执行查询完成")
        
    def test_get_database_adapter(self):
        """测试获取数据库适配器"""
        print("测试获取数据库适配器...")
        sqlite_adapter = get_database_adapter('sqlite')
        self.assertIsInstance(sqlite_adapter, SQLiteAdapter)
        
        mysql_adapter = get_database_adapter('mysql')
        self.assertIsInstance(mysql_adapter, MySQLAdapter)
        
        postgresql_adapter = get_database_adapter('postgresql')
        self.assertIsInstance(postgresql_adapter, PostgreSQLAdapter)
        
        with self.assertRaises(ValueError):
            get_database_adapter('unsupported_db')
        print("测试获取数据库适配器完成")
            

class TestTableComparator(unittest.TestCase):
    """测试表对比器类"""
    
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
            CREATE TABLE employees_old (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                age INTEGER,
                department TEXT,
                created_at TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE employees_new (
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
        conn.execute("INSERT INTO employees_old (name, email, age, department, created_at) VALUES (?, ?, ?, ?, ?)",
                     ("John Doe", "john@example.com", 30, "IT", "2023-01-01"))
        conn.execute("INSERT INTO employees_old (name, email, age, department, created_at) VALUES (?, ?, ?, ?, ?)",
                     ("Jane Smith", "jane@example.com", 25, "HR", "2023-01-02"))
        
        conn.execute("INSERT INTO employees_new (name, email, age, department, phone, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("John Doe", "john@example.com", 30, "IT", "1234567890", "2023-01-01"))
        conn.execute("INSERT INTO employees_new (name, email, age, department, phone, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                     ("Jane Smith", "jane@example.com", 26, "HR", "0987654321", "2023-01-02"))  # 年龄不同
        
        conn.commit()
        conn.close()
        
    def test_set_tables(self):
        """测试设置表名"""
        print("测试设置表名...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.assertEqual(self.comparator.table1, 'employees_old')
        self.assertEqual(self.comparator.table2, 'employees_new')
        print("测试设置表名完成")
        
    def test_set_fields(self):
        """测试设置字段"""
        print("测试设置字段...")
        fields = ['name', 'email', 'age']
        self.comparator.set_fields(fields)
        self.assertEqual(self.comparator.fields, fields)
        print("测试设置字段完成")
        
    def test_set_exclude_fields(self):
        """测试设置排除字段"""
        print("测试设置排除字段...")
        exclude_fields = ['phone', 'created_at']
        self.comparator.set_exclude_fields(exclude_fields)
        self.assertEqual(self.comparator.exclude_fields, exclude_fields)
        print("测试设置排除字段完成")
        
    def test_set_where_condition(self):
        """测试设置WHERE条件"""
        print("测试设置WHERE条件...")
        where_condition = "age > 25"
        self.comparator.set_where_condition(where_condition)
        self.assertEqual(self.comparator.where_condition, where_condition)
        print("测试设置WHERE条件完成")
        
    def test_get_table_fields(self):
        """测试获取表字段"""
        print("测试获取表字段...")
        fields = self.comparator.get_table_fields('employees_old')
        expected_fields = ['id', 'name', 'email', 'age', 'department', 'created_at']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
        print("测试获取表字段完成")
        
    def test_get_comparison_fields_all_fields(self):
        """测试获取所有对比字段（默认情况）"""
        print("测试获取所有对比字段...")
        self.comparator.set_tables('employees_old', 'employees_new')
        fields = self.comparator.get_comparison_fields()
        # 应该返回两个表的公共字段，不包括employees_new特有的phone字段
        expected_fields = ['id', 'name', 'email', 'age', 'department', 'created_at']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
        print("测试获取所有对比字段完成")
        
    def test_get_comparison_fields_specific_fields(self):
        """测试获取指定对比字段"""
        print("测试获取指定对比字段...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_fields(['name', 'email', 'age'])
        fields = self.comparator.get_comparison_fields()
        expected_fields = ['name', 'email', 'age']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
        print("测试获取指定对比字段完成")
        
    def test_get_comparison_fields_exclude_fields(self):
        """测试获取排除字段后的对比字段"""
        print("测试获取排除字段后的对比字段...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_exclude_fields(['created_at'])
        fields = self.comparator.get_comparison_fields()
        # 应该返回除created_at外的所有公共字段
        expected_fields = ['id', 'name', 'email', 'age', 'department']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
        print("测试获取排除字段后的对比字段完成")
        
    def test_build_query_without_where(self):
        """测试构建无WHERE条件的查询"""
        print("测试构建无WHERE条件的查询...")
        fields = ['id', 'name', 'email']
        query = self.comparator.build_query(fields, 'employees_old')
        # 现在查询会包含ORDER BY子句，因为表有主键
        self.assertIn("SELECT id, name, email FROM employees_old", query)
        print("测试构建无WHERE条件的查询完成")
        
    def test_build_query_with_where(self):
        """测试构建带WHERE条件的查询"""
        print("测试构建带WHERE条件的查询...")
        self.comparator.set_where_condition("age > 25")
        fields = ['id', 'name', 'email', 'age']
        query = self.comparator.build_query(fields, 'employees_old')
        # 现在查询会包含ORDER BY子句，因为表有主键
        self.assertIn("SELECT id, name, email, age FROM employees_old WHERE age > 25", query)
        print("测试构建带WHERE条件的查询完成")
        
    def test_compare_success(self):
        """测试字段不一致时的行为"""
        print("测试字段不一致时的行为...")
        self.comparator.set_tables('employees_old', 'employees_new')
        result = self.comparator.compare()
        
        # 验证结果结构
        self.assertIn('table1_row_count', result)
        self.assertIn('table2_row_count', result)
        self.assertIn('differences', result)
        self.assertIn('table1_fields', result)
        self.assertIn('table2_fields', result)
        self.assertIn('only_in_table1', result)
        self.assertIn('only_in_table2', result)
        self.assertIn('common_fields', result)
        
        # 验证行数为0（因为字段不一致，停止比较）
        self.assertEqual(result['table1_row_count'], 0)
        self.assertEqual(result['table2_row_count'], 0)
        
        # 验证差异信息
        self.assertEqual(len(result['differences']), 1)
        self.assertEqual(result['differences'][0]['type'], 'field_mismatch')
        self.assertIn('字段不一致', result['differences'][0]['message'])
        
        # 验证字段信息
        self.assertIn('phone', result['only_in_table2'])
        self.assertNotIn('phone', result['only_in_table1'])
        self.assertIn('created_at', result['common_fields'])
        print("测试字段不一致时的行为完成")
        
    def test_compare_with_where_condition(self):
        """测试带WHERE条件但字段不一致时的行为"""
        print("测试带WHERE条件但字段不一致时的行为...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_where_condition("age > 28")
        result = self.comparator.compare()
        
        # 验证行数为0（因为字段不一致，停止比较）
        self.assertEqual(result['table1_row_count'], 0)
        self.assertEqual(result['table2_row_count'], 0)
        
        # 验证差异信息
        self.assertEqual(len(result['differences']), 1)
        self.assertEqual(result['differences'][0]['type'], 'field_mismatch')
        self.assertIn('字段不一致', result['differences'][0]['message'])
        print("测试带WHERE条件但字段不一致时的行为完成")
        
    def test_compare_with_specific_fields(self):
        """测试指定字段对比"""
        print("测试指定字段对比...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_fields(['name', 'email'])
        result = self.comparator.compare()
        
        expected_fields = ['name', 'email']
        self.assertListEqual(sorted(result['fields']), sorted(expected_fields))
        print("测试指定字段对比完成")
        
    def test_compare_with_exclude_fields(self):
        """测试排除字段对比"""
        print("测试排除字段对比...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_exclude_fields(['phone', 'created_at'])
        result = self.comparator.compare()
        
        # 不应包含created_at字段（在公共字段中排除）
        expected_fields = ['id', 'name', 'email', 'age', 'department']
        self.assertListEqual(sorted(result['fields']), sorted(expected_fields))
        print("测试排除字段对比完成")
        
    def test_compare_no_common_fields(self):
        """测试没有公共字段的情况"""
        print("测试没有公共字段的情况...")
        # 创建一个特殊的适配器来模拟没有公共字段的情况
        mock_adapter = Mock()
        mock_adapter.get_table_fields.side_effect = [
            ['unique_field1'],  # table1 fields
            ['unique_field2']   # table2 fields
        ]
        
        comparator = TableComparator(mock_adapter)
        comparator.set_tables('table1', 'table2')
        
        # 当没有公共字段且未指定字段时，应该返回字段不匹配的结果
        result = comparator.compare()
        self.assertEqual(result['differences'][0]['type'], 'field_mismatch')
        print("测试没有公共字段的情况完成")
        
    def test_compare_with_fields_and_exclude(self):
        """测试同时指定字段和排除字段时，应优先使用指定字段"""
        print("测试同时指定字段和排除字段...")
        self.comparator.set_tables('employees_old', 'employees_new')
        self.comparator.set_fields(['name', 'email', 'age'])
        self.comparator.set_exclude_fields(['age'])  # 这个应该被忽略
        result = self.comparator.compare()
        
        # 应该只使用指定的字段，忽略排除字段
        expected_fields = ['name', 'email', 'age']
        self.assertListEqual(sorted(result['fields']), sorted(expected_fields))
        print("测试同时指定字段和排除字段完成")


class TestCreateSampleDatabase(unittest.TestCase):
    """测试创建示例数据库功能"""
    
    def setUp(self):
        # 创建临时文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        # 清理临时文件
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def test_create_sample_database(self):
        """测试创建示例数据库"""
        print("测试创建示例数据库...")
        create_sample_database(self.db_path)
        
        # 检查数据库文件是否存在
        self.assertTrue(os.path.exists(self.db_path))
        
        # 检查表是否创建成功
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        self.assertIn('users_old', tables)
        self.assertIn('users_new', tables)
        
        # 检查数据是否插入成功
        cursor = conn.execute("SELECT COUNT(*) FROM users_old")
        count_old = cursor.fetchone()[0]
        self.assertEqual(count_old, 2)
        
        cursor = conn.execute("SELECT COUNT(*) FROM users_new")
        count_new = cursor.fetchone()[0]
        self.assertEqual(count_new, 3)
        
        conn.close()
        print("测试创建示例数据库完成")


class TestErrorHandling(unittest.TestCase):
    """测试错误处理"""
    
    def test_compare_with_no_tables_set(self):
        """测试未设置表名时的对比"""
        print("测试未设置表名时的对比...")
        adapter = Mock()
        comparator = TableComparator(adapter)
        
        with self.assertRaises(Exception):
            comparator.compare()
        print("测试未设置表名时的对比完成")
            
    def test_compare_with_database_error(self):
        """测试数据库错误处理"""
        print("测试数据库错误处理...")
        adapter = Mock()
        adapter.get_table_fields.side_effect = Exception("数据库连接错误")
        
        comparator = TableComparator(adapter)
        comparator.set_tables('table1', 'table2')
        
        with self.assertRaises(RuntimeError) as context:
            comparator.compare()
            
        self.assertIn("对比过程中发生错误", str(context.exception))
        print("测试数据库错误处理完成")


def run_all_tests():
    """运行所有测试"""
    print("开始运行所有测试...")
    
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加测试用例
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestDatabaseAdapters))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestTableComparator))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestCreateSampleDatabase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestErrorHandling))
    
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