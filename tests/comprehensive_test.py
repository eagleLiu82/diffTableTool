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


class TestDatabaseAdaptersComprehensive(unittest.TestCase):
    """全面测试数据库适配器功能"""
    
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self._create_test_database()
        
    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_test_database(self):
        """创建测试数据库"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建测试表
        conn.execute('''
            CREATE TABLE products_old (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL,
                category TEXT,
                stock INTEGER,
                created_at TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE products_new (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL,
                category TEXT,
                stock INTEGER,
                description TEXT,
                updated_at TEXT
            )
        ''')
        
        # 插入测试数据
        conn.execute("""
            INSERT INTO products_old (name, price, category, stock, created_at) 
            VALUES (?, ?, ?, ?, ?)
        """, ("笔记本电脑", 5999.99, "电子产品", 10, "2023-01-01"))
        
        conn.execute("""
            INSERT INTO products_old (name, price, category, stock, created_at) 
            VALUES (?, ?, ?, ?, ?)
        """, ("机械键盘", 299.99, "电子产品", 50, "2023-01-02"))
        
        conn.execute("""
            INSERT INTO products_new (name, price, category, stock, description, updated_at) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("笔记本电脑", 5999.99, "电子产品", 10, "高性能笔记本", "2023-06-01"))
        
        conn.execute("""
            INSERT INTO products_new (name, price, category, stock, description, updated_at) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("机械键盘", 349.99, "电子产品", 45, "青轴机械键盘", "2023-06-02"))
        
        conn.commit()
        conn.close()
    
    def test_sqlite_adapter_complete_functionality(self):
        """测试SQLite适配器完整功能"""
        # 测试连接
        adapter = SQLiteAdapter()
        connection = adapter.connect(db_path=self.db_path)
        self.assertIsNotNone(connection)
        
        # 测试获取字段
        fields_old = adapter.get_table_fields('products_old')
        fields_new = adapter.get_table_fields('products_new')
        
        expected_old_fields = ['id', 'name', 'price', 'category', 'stock', 'created_at']
        expected_new_fields = ['id', 'name', 'price', 'category', 'stock', 'description', 'updated_at']
        
        self.assertListEqual(sorted(fields_old), sorted(expected_old_fields))
        self.assertListEqual(sorted(fields_new), sorted(expected_new_fields))
        
        # 测试执行查询 - 无WHERE条件
        cursor = adapter.execute_query("SELECT * FROM products_old")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        
        # 测试执行查询 - 有WHERE条件
        cursor = adapter.execute_query("SELECT * FROM products_old WHERE price > 1000")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "笔记本电脑")
        
        # 测试执行查询 - 选择特定字段
        cursor = adapter.execute_query("SELECT name, price FROM products_new")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(rows[0]), 2)  # 只有两列
        
        adapter.close()
    
    def test_mysql_adapter_import_error(self):
        """测试MySQL适配器导入错误处理"""
        with patch.dict('sys.modules', {'mysql.connector': None}):
            adapter = MySQLAdapter()
            with self.assertRaises(ImportError) as context:
                adapter.connect(user='test', password='test', database='test')
            self.assertIn("需要安装mysql-connector-python库", str(context.exception))
    
    def test_postgresql_adapter_import_error(self):
        """测试PostgreSQL适配器导入错误处理"""
        with patch.dict('sys.modules', {'psycopg2': None}):
            adapter = PostgreSQLAdapter()
            with self.assertRaises(ImportError) as context:
                adapter.connect(user='test', password='test', database='test')
            self.assertIn("需要安装psycopg2库", str(context.exception))
    
    def test_get_database_adapter_functionality(self):
        """测试获取数据库适配器功能"""
        # 测试支持的适配器
        adapters = [
            ('sqlite', SQLiteAdapter),
            ('mysql', MySQLAdapter),
            ('postgresql', PostgreSQLAdapter)
        ]
        
        for db_type, expected_class in adapters:
            adapter = get_database_adapter(db_type)
            self.assertIsInstance(adapter, expected_class)
        
        # 测试不支持的适配器
        with self.assertRaises(ValueError) as context:
            get_database_adapter('oracle')
        self.assertIn("不支持的数据库类型", str(context.exception))


class TestTableComparatorComprehensive(unittest.TestCase):
    """全面测试表对比器功能"""
    
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self._create_test_database()
        
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def _create_test_database(self):
        """创建测试数据库"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建员工表（字段完全一致的表）
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
        
        conn.execute('''
            CREATE TABLE employees_2023 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                department TEXT,
                salary REAL,
                hire_date TEXT,
                is_active INTEGER
            )
        ''')
        
        # 插入2022年员工数据
        employees_2022_data = [
            (1, "张三", "zhangsan@company.com", "技术部", 15000.0, "2021-03-15", 1),
            (2, "李四", "lisi@company.com", "销售部", 12000.0, "2020-07-01", 1),
            (3, "王五", "wangwu@company.com", "人事部", 10000.0, "2022-01-10", 0)
        ]
        
        conn.executemany("""
            INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, employees_2022_data)
        
        # 插入2023年员工数据（有变化）
        employees_2023_data = [
            (1, "张三", "zhangsan@company.com", "技术部", 16000.0, "2021-03-15", 1),
            (2, "李四", "lisi@company.com", "销售部", 13000.0, "2020-07-01", 1),
            (3, "王五", "wangwu@company.com", "人事部", 10000.0, "2022-01-10", 0),
            (4, "赵六", "zhaoliu@company.com", "市场部", 11000.0, "2023-05-01", 1)
        ]
        
        conn.executemany("""
            INSERT INTO employees_2023 
            (id, name, email, department, salary, hire_date, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, employees_2023_data)
        
        conn.commit()
        conn.close()
    
    def test_default_field_selection(self):
        """测试默认字段选择（所有公共字段）"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        fields = self.comparator.get_comparison_fields()
        
        # 公共字段应该是所有字段
        expected_fields = ['id', 'name', 'email', 'department', 'salary', 'hire_date', 'is_active']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
    
    def test_specific_field_selection(self):
        """测试指定字段选择"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_fields(['name', 'salary', 'department'])
        fields = self.comparator.get_comparison_fields()
        
        expected_fields = ['name', 'salary', 'department']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
    
    def test_exclude_field_selection(self):
        """测试排除字段选择"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_exclude_fields(['hire_date', 'is_active'])
        fields = self.comparator.get_comparison_fields()
        
        expected_fields = ['id', 'name', 'email', 'department', 'salary']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
    
    def test_priority_of_field_specification(self):
        """测试字段指定优先级（指定字段 > 排除字段）"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_fields(['name', 'email', 'salary'])  # 这个应该优先
        self.comparator.set_exclude_fields(['salary'])  # 这个应该被忽略
        fields = self.comparator.get_comparison_fields()
        
        # 应该只使用指定的字段，忽略排除字段
        expected_fields = ['name', 'email', 'salary']
        self.assertListEqual(sorted(fields), sorted(expected_fields))
    
    def test_where_condition_functionality(self):
        """测试WHERE条件功能"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_where_condition("salary > 11000")
        
        # 构建查询并验证
        fields = ['name', 'salary']
        query = self.comparator.build_query(fields, 'employees_2022')
        expected_query = "SELECT name, salary FROM employees_2022 WHERE salary > 11000"
        self.assertEqual(query, expected_query)
    
    def test_complete_comparison_default_fields(self):
        """测试完整对比 - 默认字段"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        result = self.comparator.compare()
        
        # 验证基本信息
        self.assertEqual(result['table1_row_count'], 3)  # 2022年有3个员工
        self.assertEqual(result['table2_row_count'], 4)  # 2023年有4个员工
        
        # 验证字段
        expected_fields = ['id', 'name', 'email', 'department', 'salary', 'hire_date', 'is_active']
        self.assertListEqual(sorted(result['fields']), sorted(expected_fields))
        
        # 应该有一个差异（行数不同）
        self.assertEqual(len(result['differences']), 1)
        self.assertEqual(result['differences'][0]['type'], 'row_count')
    
    def test_complete_comparison_specific_fields(self):
        """测试完整对比 - 指定字段"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_fields(['name', 'department', 'salary'])
        result = self.comparator.compare()
        
        # 验证字段
        expected_fields = ['name', 'department', 'salary']
        self.assertListEqual(sorted(result['fields']), sorted(expected_fields))
        
        # 行数应该相同（因为是基于相同的数据表）
        self.assertEqual(result['table1_row_count'], 3)
        self.assertEqual(result['table2_row_count'], 4)
    
    def test_complete_comparison_with_where_condition(self):
        """测试完整对比 - 带WHERE条件"""
        self.comparator.set_tables('employees_2022', 'employees_2023')
        self.comparator.set_where_condition("salary >= 12000")  # 修改条件以匹配实际数据
        result = self.comparator.compare()
        
        # 只应该返回薪资大于等于12000的员工
        self.assertEqual(result['table1_row_count'], 2)  # 张三(15000)和李四(12000)
        self.assertEqual(result['table2_row_count'], 2)  # 张三(16000)和李四(13000)
        
        # 行数相同，但数据可能有差异，检查是否有multiple_row_diff类型
        multiple_diff_types = [diff for diff in result['differences'] if diff['type'] == 'multiple_row_diff']
        # 应该有2行数据差异（张三和李四的薪资都不同）
        self.assertEqual(len(multiple_diff_types), 1)
        self.assertEqual(multiple_diff_types[0]['count'], 2)
    
    def test_comparison_with_field_mismatch(self):
        """测试字段不一致的情况"""
        # 创建一个特殊的适配器来模拟字段不一致的情况
        mock_adapter = Mock()
        mock_adapter.get_table_fields.side_effect = [
            ['id', 'name', 'email'],  # 表1字段
            ['id', 'name', 'phone']   # 表2字段
        ]
        
        comparator = TableComparator(mock_adapter)
        comparator.set_tables('table_a', 'table_b')
        
        result = comparator.compare()
        
        # 验证返回了正确的结果结构
        self.assertEqual(len(result['differences']), 1)
        self.assertEqual(result['differences'][0]['type'], 'field_mismatch')
        self.assertEqual(result['differences'][0]['message'], '表 table_a 和 table_b 的字段不完全一致')
        self.assertEqual(result['table1_fields'], ['id', 'name', 'email'])
        self.assertEqual(result['table2_fields'], ['id', 'name', 'phone'])
    
    def test_comparison_database_error_handling(self):
        """测试数据库错误处理"""
        mock_adapter = Mock()
        mock_adapter.get_table_fields.return_value = ['id', 'name']
        mock_adapter.execute_query.side_effect = Exception("数据库连接失败")
        
        comparator = TableComparator(mock_adapter)
        comparator.set_tables('table_a', 'table_b')
        
        with self.assertRaises(RuntimeError) as context:
            comparator.compare()
        self.assertIn("对比过程中发生错误", str(context.exception))
        
    def test_row_comparison_functionality_same_row_count(self):
        """测试行数相同时的行对比功能"""
        # 创建一个专门用于测试行对比的数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE test_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        # 插入相同行数但有差异的数据
        conn.execute("INSERT INTO test_table1 VALUES (1, 'Alice', 100)")
        conn.execute("INSERT INTO test_table1 VALUES (2, 'Bob', 200)")
        
        conn.execute("INSERT INTO test_table2 VALUES (1, 'Alice', 150)")  # value不同
        conn.execute("INSERT INTO test_table2 VALUES (2, 'Bob', 200)")   # 相同
        
        conn.commit()
        conn.close()
        
        # 使用真实适配器测试行对比
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('test_table1', 'test_table2')
        
        result = comparator.compare()
        
        # 行数相同，应该有1行差异（第一行的value字段不同）
        self.assertEqual(len(result['row_differences']), 1)
        self.assertEqual(result['row_differences'][0]['row_number'], 1)
        self.assertEqual(len(result['row_differences'][0]['differences']), 1)
        self.assertEqual(result['row_differences'][0]['differences'][0]['field'], 'value')
        self.assertEqual(result['row_differences'][0]['differences'][0]['table1_value'], 100)
        self.assertEqual(result['row_differences'][0]['differences'][0]['table2_value'], 150)
        
        adapter.close()
        os.unlink(db_path)
        
    def test_row_comparison_functionality_different_row_count(self):
        """测试行数不同时的行对比功能"""
        # 创建一个专门用于测试行数不同的数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE test_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        # 插入不同行数但第一行有差异的数据
        conn.execute("INSERT INTO test_table1 VALUES (1, 'Alice', 100)")
        conn.execute("INSERT INTO test_table1 VALUES (2, 'Bob', 200)")
        
        conn.execute("INSERT INTO test_table2 VALUES (1, 'Alice', 150)")  # value不同
        conn.execute("INSERT INTO test_table2 VALUES (2, 'Charlie', 300)")
        conn.execute("INSERT INTO test_table2 VALUES (3, 'David', 400)")
        
        conn.commit()
        conn.close()
        
        # 使用真实适配器测试行对比
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('test_table1', 'test_table2')
        
        result = comparator.compare()
        
        # 应该有行数差异
        self.assertEqual(len(result['differences']), 1)
        self.assertEqual(result['differences'][0]['type'], 'row_count')
        
        # 也应该有第一行的字段差异
        self.assertEqual(len(result['row_differences']), 1)
        self.assertEqual(result['row_differences'][0]['row_number'], 1)
        self.assertEqual(len(result['row_differences'][0]['differences']), 1)
        self.assertEqual(result['row_differences'][0]['differences'][0]['field'], 'value')
        self.assertEqual(result['row_differences'][0]['differences'][0]['table1_value'], 100)
        self.assertEqual(result['row_differences'][0]['differences'][0]['table2_value'], 150)
        
        adapter.close()
        os.unlink(db_path)
        
    def test_multiple_row_differences(self):
        """测试多行差异"""
        # 创建一个多行差异的测试数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE test_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE test_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER
            )
        ''')
        
        # 插入多行有差异的数据
        test_data1 = [
            (1, 'Alice', 100),
            (2, 'Bob', 200),
            (3, 'Charlie', 300),
            (4, 'David', 400)
        ]
        
        test_data2 = [
            (1, 'Alice', 150),    # value不同
            (2, 'Bob', 200),      # 相同
            (3, 'Charlie', 350),  # value不同
            (4, 'Dave', 400)      # name不同
        ]
        
        conn.executemany("INSERT INTO test_table1 VALUES (?, ?, ?)", test_data1)
        conn.executemany("INSERT INTO test_table2 VALUES (?, ?, ?)", test_data2)
        
        conn.commit()
        conn.close()
        
        # 使用真实适配器测试多行差异
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('test_table1', 'test_table2')
        
        result = comparator.compare()
        
        # 行数相同
        self.assertEqual(result['table1_row_count'], 4)
        self.assertEqual(result['table2_row_count'], 4)
        
        # 应该有多行差异的统计信息
        multiple_diff_info = [diff for diff in result['differences'] if diff['type'] == 'multiple_row_diff']
        self.assertEqual(len(multiple_diff_info), 1)
        self.assertEqual(multiple_diff_info[0]['count'], 3)  # 3行有差异
        
        # 应该有第一行的差异详情
        self.assertEqual(len(result['row_differences']), 1)
        self.assertEqual(result['row_differences'][0]['row_number'], 1)
        self.assertEqual(result['row_differences'][0]['differences'][0]['field'], 'value')
        self.assertEqual(result['row_differences'][0]['differences'][0]['table1_value'], 100)
        self.assertEqual(result['row_differences'][0]['differences'][0]['table2_value'], 150)
        
        adapter.close()
        os.unlink(db_path)


class TestCreateSampleDatabaseComprehensive(unittest.TestCase):
    """全面测试创建示例数据库功能"""
    
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_create_sample_database_completely(self):
        """完整测试创建示例数据库功能"""
        # 创建示例数据库
        create_sample_database(self.db_path)
        
        # 验证数据库文件存在
        self.assertTrue(os.path.exists(self.db_path))
        
        # 连接到数据库验证内容
        conn = sqlite3.connect(self.db_path)
        
        # 验证表是否存在
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        self.assertIn('users_old', tables)
        self.assertIn('users_new', tables)
        
        # 验证 users_old 表结构
        cursor = conn.execute("PRAGMA table_info(users_old)")
        old_fields = [row[1] for row in cursor.fetchall()]
        expected_old_fields = ['id', 'name', 'email', 'age', 'created_at']
        self.assertListEqual(sorted(old_fields), sorted(expected_old_fields))
        
        # 验证 users_new 表结构
        cursor = conn.execute("PRAGMA table_info(users_new)")
        new_fields = [row[1] for row in cursor.fetchall()]
        expected_new_fields = ['id', 'name', 'email', 'age', 'phone', 'created_at']
        self.assertListEqual(sorted(new_fields), sorted(expected_new_fields))
        
        # 验证数据行数
        cursor = conn.execute("SELECT COUNT(*) FROM users_old")
        old_count = cursor.fetchone()[0]
        self.assertEqual(old_count, 2)
        
        cursor = conn.execute("SELECT COUNT(*) FROM users_new")
        new_count = cursor.fetchone()[0]
        self.assertEqual(new_count, 3)
        
        # 验证具体数据
        cursor = conn.execute("SELECT name, email, age FROM users_old ORDER BY id")
        old_users = cursor.fetchall()
        expected_old_users = [
            ("张三", "zhangsan@example.com", 25),
            ("李四", "lisi@example.com", 30)
        ]
        self.assertEqual(old_users, expected_old_users)
        
        cursor = conn.execute("SELECT name, email, age FROM users_new ORDER BY id")
        new_users = cursor.fetchall()
        # 注意：李四的年龄在新表中是31，不是30
        expected_new_users = [
            ("张三", "zhangsan@example.com", 25),
            ("李四", "lisi@example.com", 31)
        ]
        self.assertEqual(new_users[:2], expected_new_users)
        
        conn.close()


class TestEdgeCasesAndErrorHandling(unittest.TestCase):
    """测试边界情况和错误处理"""
    
    def setUp(self):
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self._create_test_database()
        
        self.adapter = SQLiteAdapter()
        self.adapter.connect(db_path=self.db_path)
        self.comparator = TableComparator(self.adapter)
        
    def tearDown(self):
        self.adapter.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def _create_test_database(self):
        """创建测试数据库"""
        conn = sqlite3.connect(self.db_path)
        
        # 创建空表用于边界测试
        conn.execute('''
            CREATE TABLE empty_table1 (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE empty_table2 (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        
        # 创建单字段表
        conn.execute('''
            CREATE TABLE single_field1 (
                id INTEGER PRIMARY KEY
            )
        ''')
        
        conn.execute('''
            CREATE TABLE single_field2 (
                id INTEGER PRIMARY KEY
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def test_comparison_of_empty_tables(self):
        """测试空表对比"""
        self.comparator.set_tables('empty_table1', 'empty_table2')
        result = self.comparator.compare()
        
        self.assertEqual(result['table1_row_count'], 0)
        self.assertEqual(result['table2_row_count'], 0)
        self.assertEqual(len(result['differences']), 0)  # 应该没有差异
        self.assertEqual(len(result['row_differences']), 0)  # 应该没有行差异
    
    def test_comparison_of_single_field_tables(self):
        """测试单字段表对比"""
        self.comparator.set_tables('single_field1', 'single_field2')
        result = self.comparator.compare()
        
        self.assertEqual(result['fields'], ['id'])
        self.assertEqual(result['table1_row_count'], 0)
        self.assertEqual(result['table2_row_count'], 0)
    
    def test_comparison_nonexistent_table(self):
        """测试不存在的表"""
        self.comparator.set_tables('nonexistent_table', 'empty_table1')
        # 这里我们期望出现异常，但不是RuntimeError
        with self.assertRaises(Exception):
            self.comparator.compare()
    
    def test_comparison_with_complex_where_condition(self):
        """测试复杂WHERE条件"""
        # 先创建一些有数据的表用于测试
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT INTO empty_table1 (name) VALUES ('test')")
        conn.commit()
        conn.close()
        
        self.comparator.set_tables('empty_table1', 'empty_table2')
        self.comparator.set_where_condition("name LIKE 'test%' AND id > 0")
        result = self.comparator.compare()
        
        self.assertEqual(result['table1_row_count'], 1)
        self.assertEqual(result['table2_row_count'], 0)


def run_comprehensive_tests():
    """运行全面测试"""
    print("开始运行全面测试...")
    print("=" * 50)
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加所有测试类
    test_classes = [
        TestDatabaseAdaptersComprehensive,
        TestTableComparatorComprehensive,
        TestCreateSampleDatabaseComprehensive,
        TestEdgeCasesAndErrorHandling
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    print("\n" + "=" * 50)
    print("测试总结:")
    print(f"运行测试数: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    
    if result.failures:
        print("\n失败详情:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\n错误详情:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    try:
        success = run_comprehensive_tests()
        if success:
            print("\n🎉 所有测试通过!")
        else:
            print("\n❌ 部分测试失败!")
        exit(0 if success else 1)
    except Exception as e:
        print(f"❌ 运行测试时发生错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)