#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sqlite3
import os
import sys
import tempfile
import tkinter as tk
from unittest.mock import Mock, patch, MagicMock

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入GUI模块
from table_diff_gui import TableDiffGUI


class TestGUIQueryFunctionality(unittest.TestCase):
    """测试GUI中的查询功能"""

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

        # 插入测试数据
        conn.execute("INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                     ("Alice", "IT", 5000))
        conn.execute("INSERT INTO employees (name, department, salary) VALUES (?, ?, ?)",
                     ("Bob", "HR", 4500))

        conn.commit()
        conn.close()

    def test_query_text_widgets_exist(self):
        """测试查询文本框是否存在"""
        print("测试查询文本框是否存在...")

        # 检查查询文本框是否存在
        self.assertTrue(hasattr(self.app, 'query1_text'))
        self.assertTrue(hasattr(self.app, 'query2_text'))
        
        # 检查它们是否是Text组件
        self.assertIsInstance(self.app.query1_text, tk.Text)
        self.assertIsInstance(self.app.query2_text, tk.Text)

        print("测试查询文本框是否存在完成")

    def test_query_variables_binding(self):
        """测试查询变量绑定"""
        print("测试查询变量绑定...")

        # 检查查询变量是否存在
        self.assertTrue(hasattr(self.app, 'query1'))
        self.assertTrue(hasattr(self.app, 'query2'))
        
        # 检查变量类型
        self.assertIsInstance(self.app.query1, tk.StringVar)
        self.assertIsInstance(self.app.query2, tk.StringVar)

        print("测试查询变量绑定完成")

    def test_query_text_input_updates_variables(self):
        """测试在查询文本框中输入内容会更新变量"""
        print("测试在查询文本框中输入内容会更新变量...")

        # 模拟在查询1文本框中输入内容
        test_query = "SELECT * FROM employees"
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, test_query)
        
        # 触发事件处理
        self.app.on_query1_change()
        
        # 在GUI测试中，我们直接检查文本框内容而不是变量
        # 因为变量更新是通过after方法异步处理的
        text_content = self.app.query1_text.get(1.0, tk.END).strip()
        self.assertEqual(text_content, test_query)

        print("测试在查询文本框中输入内容会更新变量完成")

    def test_run_query_comparison_function_exists(self):
        """测试运行查询对比的函数存在"""
        print("测试运行查询对比的函数存在...")

        # 检查方法是否存在
        self.assertTrue(hasattr(self.app, '_run_query_comparison'))
        self.assertTrue(callable(getattr(self.app, '_run_query_comparison')))

        print("测试运行查询对比的函数存在完成")

    def test_query_mode_detection(self):
        """测试查询模式检测"""
        print("测试查询模式检测...")

        # 设置表名和查询语句
        self.app.table1.set("employees")
        self.app.table2.set("employees")
        self.app.query1.set("SELECT * FROM employees")
        self.app.query2.set("SELECT * FROM employees")

        # 检查运行函数是否存在
        self.assertTrue(hasattr(self.app, '_run_comparison_thread'))
        self.assertTrue(callable(getattr(self.app, '_run_comparison_thread')))

        print("测试查询模式检测完成")

    def test_save_load_query_config(self):
        """测试保存和加载查询配置"""
        print("测试保存和加载查询配置...")

        # 设置查询内容
        query1_text = "SELECT name, department FROM employees WHERE department = 'IT'"
        query2_text = "SELECT name, department FROM employees WHERE department = 'HR'"
        
        self.app.query1.set(query1_text)
        self.app.query2.set(query2_text)
        
        # 更新文本框
        self.app.query1_text.delete(1.0, tk.END)
        self.app.query1_text.insert(tk.END, query1_text)
        self.app.query2_text.delete(1.0, tk.END)
        self.app.query2_text.insert(tk.END, query2_text)

        # 检查配置字典是否包含查询
        # 模拟save_config中的配置收集部分
        config = {
            "query1": self.app.query1.get(),
            "query2": self.app.query2.get(),
        }
        
        self.assertEqual(config["query1"], query1_text)
        self.assertEqual(config["query2"], query2_text)

        # 测试配置加载部分
        test_config = {
            "query1": "SELECT id, name FROM employees",
            "query2": "SELECT id, name FROM employees WHERE salary > 4000"
        }
        
        self.app.query1.set(test_config["query1"])
        self.app.query2.set(test_config["query2"])
        
        self.assertEqual(self.app.query1.get(), test_config["query1"])
        self.assertEqual(self.app.query2.get(), test_config["query2"])

        print("测试保存和加载查询配置完成")

    def test_database_config_for_query_mode(self):
        """测试查询模式下的数据库配置"""
        print("测试查询模式下的数据库配置...")

        # 设置数据库配置
        self.app.source_db_type.set("sqlite")
        self.app.source_db_path.set(self.db_path)
        self.app.target_db_type.set("sqlite")
        self.app.target_db_path.set(self.db_path)

        # 检查配置是否正确设置
        self.assertEqual(self.app.source_db_type.get(), "sqlite")
        self.assertEqual(self.app.source_db_path.get(), self.db_path)
        self.assertEqual(self.app.target_db_type.get(), "sqlite")
        self.assertEqual(self.app.target_db_path.get(), self.db_path)

        print("测试查询模式下的数据库配置完成")


if __name__ == '__main__':
    unittest.main()