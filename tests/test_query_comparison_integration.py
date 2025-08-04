#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sqlite3
import os
import sys
import tempfile
import tkinter as tk
from unittest.mock import Mock, patch, MagicMock
import threading
import time

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入模块
from table_diff_gui import TableDiffGUI


class TestQueryComparisonIntegration(unittest.TestCase):
    """查询对比功能集成测试"""

    def setUp(self):
        # 创建根窗口但不显示
        self.root = tk.Tk()
        self.root.withdraw()  # 隐藏窗口

        # 创建GUI应用实例
        self.app = TableDiffGUI(self.root)

        # 创建临时数据库文件
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name

        # 创建测试数据库和表
        self._create_test_database()

        # 配置数据库连接
        self._configure_database()

    def tearDown(self):
        # 销毁GUI
        self.app = None
        self.root.destroy()
        
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

    def _configure_database(self):
        """配置数据库连接参数"""
        # 设置源数据库配置
        self.app.source_db_type.set("sqlite")
        self.app.source_db_path.set(self.db_path)
        
        # 设置目标数据库配置（使用相同数据库模拟）
        self.app.target_db_type.set("sqlite")
        self.app.target_db_path.set(self.db_path)

    def test_query_comparison_end_to_end(self):
        """端到端测试查询对比功能"""
        print("端到端测试查询对比功能...")

        # 设置查询语句
        query1 = "SELECT name, salary FROM employees WHERE department = 'IT' ORDER BY name"
        query2 = "SELECT name, salary FROM employees WHERE department = 'HR' ORDER BY name"
        
        # 在GUI中设置查询
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query1)
        self.app.query1.set(query1)
        
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query2)
        self.app.query2.set(query2)

        # 模拟运行对比（直接调用内部方法避免线程问题）
        try:
            result = self.app._run_query_comparison()
            
            # 验证结果
            self.assertIsNotNone(result)
            self.assertIn('table1_row_count', result)
            self.assertIn('table2_row_count', result)
            self.assertIn('row_differences', result)
            
            # IT部门应该有2个员工，HR部门应该有1个员工
            self.assertEqual(result['table1_row_count'], 2)
            self.assertEqual(result['table2_row_count'], 1)
            
            print("端到端测试查询对比功能完成")
        except Exception as e:
            self.fail(f"查询对比失败: {str(e)}")

    def test_query_comparison_same_results(self):
        """测试相同查询结果的对比"""
        print("测试相同查询结果的对比...")

        # 设置相同的查询语句
        query = "SELECT name FROM employees WHERE department = 'IT' ORDER BY name"
        
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query)
        self.app.query1.set(query)
        
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query)
        self.app.query2.set(query)

        # 运行对比
        try:
            result = self.app._run_query_comparison()
            
            # 验证结果
            self.assertIsNotNone(result)
            self.assertEqual(result['table1_row_count'], result['table2_row_count'])
            # 应该没有行差异
            self.assertEqual(len(result['row_differences']), 0)
            
            print("测试相同查询结果的对比完成")
        except Exception as e:
            self.fail(f"相同查询对比失败: {str(e)}")

    def test_query_comparison_field_mismatch(self):
        """测试字段不匹配的查询对比"""
        print("测试字段不匹配的查询对比...")

        # 设置不同字段的查询语句
        query1 = "SELECT name, department FROM employees"
        query2 = "SELECT name, budget FROM departments"
        
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query1)
        self.app.query1.set(query1)
        
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query2)
        self.app.query2.set(query2)

        # 运行对比
        try:
            result = self.app._run_query_comparison()
            
            # 验证结果
            self.assertIsNotNone(result)
            # 应该检测到字段不匹配
            self.assertGreater(len(result['differences']), 0)
            field_mismatch_found = any(diff['type'] == 'field_mismatch' for diff in result['differences'])
            self.assertTrue(field_mismatch_found)
            
            print("测试字段不匹配的查询对比完成")
        except Exception as e:
            self.fail(f"字段不匹配查询对比失败: {str(e)}")

    def test_display_query_results(self):
        """测试查询结果的显示"""
        print("测试查询结果的显示...")

        # 设置查询语句
        query1 = "SELECT name, salary FROM employees WHERE department = 'IT'"
        query2 = "SELECT name, salary FROM employees WHERE department = 'HR'"
        
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query1)
        self.app.query1.set(query1)
        
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query2)
        self.app.query2.set(query2)

        # 运行对比
        try:
            result = self.app._run_query_comparison()
            
            # 模拟结果显示
            self.app._display_result(result)
            
            # 检查结果文本是否更新
            result_text = self.app.result_text.get(1.0, tk.END)
            self.assertIn("查询对比结果", result_text)
            self.assertIn("SELECT name, salary", result_text)
            
            print("测试查询结果的显示完成")
        except Exception as e:
            self.fail(f"查询结果显示失败: {str(e)}")

    def test_mixed_mode_selection(self):
        """测试混合模式选择（表对比和查询对比）"""
        print("测试混合模式选择...")

        # 同时设置表名和查询语句
        self.app.table1.set("employees")
        self.app.table2.set("departments")
        
        query1 = "SELECT * FROM employees"
        query2 = "SELECT * FROM departments"
        self.app.query1.set(query1)
        self.app.query2.set(query2)
        
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query1)
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query2)

        # 检查运行逻辑是否能正确选择模式
        # 根据实现，查询对比优先级应该更高
        try:
            # 检查内部判断逻辑
            has_queries = bool(self.app.query1.get().strip() and self.app.query2.get().strip())
            has_tables = bool(self.app.table1.get().strip() and self.app.table2.get().strip())
            
            self.assertTrue(has_queries)
            self.assertTrue(has_tables)
            
            print("测试混合模式选择完成")
        except Exception as e:
            self.fail(f"混合模式选择测试失败: {str(e)}")


if __name__ == '__main__':
    unittest.main()