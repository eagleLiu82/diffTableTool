#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import json
from typing import List, Dict, Any, Optional
import sys
import os
import sqlite3

# 添加当前目录到Python路径，以便导入table_diff
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from table_diff import run_comparison, get_database_adapter
    TABLE_DIFF_AVAILABLE = True
except ImportError:
    TABLE_DIFF_AVAILABLE = False


class QueryComparator:
    """查询对比器，用于执行和对比两个自定义查询的结果"""
    
    def __init__(self, source_db_adapter, target_db_adapter=None):
        self.source_db = source_db_adapter
        self.target_db = target_db_adapter if target_db_adapter else source_db_adapter

    def compare_queries(self, query1: str, query2: str) -> Dict[str, Any]:
        """
        对比两个自定义查询的结果
        
        :param query1: 第一个查询语句
        :param query2: 第二个查询语句
        :return: 对比结果
        """
        # 执行查询
        cursor1 = self.source_db.execute_query(query1)
        rows1 = cursor1.fetchall()
        
        cursor2 = self.target_db.execute_query(query2)
        rows2 = cursor2.fetchall()
        
        # 获取列名
        if hasattr(cursor1, 'description'):
            columns1 = [desc[0] for desc in cursor1.description]
        else:
            # 对于某些数据库，可能需要特殊处理
            columns1 = [f"column_{i}" for i in range(len(rows1[0]) if rows1 else 0)]
            
        if hasattr(cursor2, 'description'):
            columns2 = [desc[0] for desc in cursor2.description]
        else:
            columns2 = [f"column_{i}" for i in range(len(rows2[0]) if rows2 else 0)]
        
        # 准备结果
        result = {
            'fields': columns1,  # 使用第一个查询的列名
            'table1_row_count': len(rows1),
            'table2_row_count': len(rows2),
            'differences': [],
            'row_differences': [],
            'table1_fields': columns1,
            'table2_fields': columns2,
            'query1': query1,
            'query2': query2
        }
        
        # 检查列是否一致
        if columns1 != columns2:
            result['differences'].append({
                'type': 'field_mismatch',
                'message': '两个查询的列名不一致',
                'details': {
                    'table1_fields': columns1,
                    'table2_fields': columns2,
                    'only_in_table1': [c for c in columns1 if c not in columns2],
                    'only_in_table2': [c for c in columns2 if c not in columns1],
                    'common_fields': [c for c in columns1 if c in columns2]
                }
            })
            return result
        
        # 将行数据转换为字典列表以便比较
        dict_rows1 = [dict(zip(columns1, row)) for row in rows1]
        dict_rows2 = [dict(zip(columns2, row)) for row in rows2]
        
        # 按行位置进行对比
        result['row_differences'] = self._compare_rows_by_position(dict_rows1, dict_rows2, columns1)
        
        # 添加差异计数信息
        diff_count = len(result['row_differences'])
        if diff_count > 0:
            result['differences'].append({
                'type': 'multiple_row_diff',
                'count': diff_count,
                'message': f'共有{diff_count}行存在数据差异'
            })
            
        return result

    def _compare_rows_by_position(self, rows1: List[Dict], rows2: List[Dict], 
                                  comparison_fields: List[str]) -> List[Dict]:
        """
        基于行位置对比两组行数据
        
        :param rows1: 第一组行数据
        :param rows2: 第二组行数据
        :param comparison_fields: 需要对比的字段列表
        :return: 差异列表
        """
        differences = []
        
        # 处理共同行数范围内的对比
        for i in range(min(len(rows1), len(rows2))):
            row_diff = self._compare_single_row(rows1[i], rows2[i], i+1, comparison_fields)
            if row_diff:
                row_diff['type'] = 'different_data'
                differences.append(row_diff)
        
        # 处理多余的行（如果有的话）
        if len(rows1) != len(rows2):
            # 标记多余行
            if len(rows1) > len(rows2):
                for i in range(len(rows2), len(rows1)):
                    differences.append({
                        'row_number': i+1,
                        'type': 'only_in_table1',
                        'differences': [{'field': field, 'table1_value': rows1[i][field], 'table2_value': None} 
                                       for field in comparison_fields]
                    })
            else:
                for i in range(len(rows1), len(rows2)):
                    differences.append({
                        'row_number': i+1,
                        'type': 'only_in_table2',
                        'differences': [{'field': field, 'table1_value': None, 'table2_value': rows2[i][field]} 
                                       for field in comparison_fields]
                    })
        
        return differences

    def _compare_single_row(self, row1: Dict, row2: Dict, row_number: int, 
                            comparison_fields: List[str]) -> Optional[Dict]:
        """
        对比单行数据
        
        :param row1: 第一行数据
        :param row2: 第二行数据
        :param row_number: 行号
        :param comparison_fields: 需要对比的字段列表
        :return: 差异信息，如果没有差异则返回None
        """
        differences = []
        
        for field in comparison_fields:
            value1 = row1.get(field)
            value2 = row2.get(field)
            
            if value1 != value2:
                differences.append({
                    'field': field,
                    'table1_value': value1,
                    'table2_value': value2
                })
        
        if differences:
            return {
                'row_number': row_number,
                'differences': differences
            }
        
        return None

    def generate_csv_report(self, result: Dict[str, Any], output_file: str) -> None:
        """
        生成CSV格式的详细差异报告
        
        :param result: 对比结果
        :param output_file: 输出文件路径
        """
        import csv
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['row_type', 'row_number', 'column_name', 'table1_value', 'table2_value']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # 遍历所有行差异
            if 'row_differences' in result:
                for row_diff in result['row_differences']:
                    row_type = row_diff.get('type', 'unknown')
                    row_number = row_diff['row_number']
                    
                    for diff in row_diff['differences']:
                        csv_row = {
                            'row_type': row_type,
                            'row_number': row_number,
                            'column_name': diff['field'],
                            'table1_value': diff['table1_value'],
                            'table2_value': diff['table2_value']
                        }
                        writer.writerow(csv_row)


class TableDiffGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("数据库表对比工具")
        self.root.geometry("1000x800")  # 增大窗口尺寸
        
        # 创建变量
        self.setup_variables()
        
        # 创建界面
        self.create_widgets()
        
        # 设置样式
        self.setup_styles()
        
        # 标记是否正在同步配置，防止循环更新
        self.syncing_configs = False
        
    def setup_variables(self):
        # 数据库类型变量
        self.source_db_type = tk.StringVar(value="postgresql")
        self.target_db_type = tk.StringVar(value="postgresql")
        
        # 源数据库连接参数
        self.source_db_path = tk.StringVar()
        self.source_host = tk.StringVar(value="localhost")
        self.source_port = tk.StringVar()
        self.source_user = tk.StringVar()
        self.source_password = tk.StringVar()
        self.source_database = tk.StringVar()
        
        # 目标数据库连接参数
        self.target_db_path = tk.StringVar()
        self.target_host = tk.StringVar(value="localhost")
        self.target_port = tk.StringVar()
        self.target_user = tk.StringVar()
        self.target_password = tk.StringVar()
        self.target_database = tk.StringVar()
        
        # 表名和字段参数
        self.table1 = tk.StringVar()
        self.table2 = tk.StringVar()
        self.fields = tk.StringVar()
        self.exclude = tk.StringVar()
        self.where_condition = tk.StringVar()
        # 添加两个表的独立WHERE条件
        self.where_condition1 = tk.StringVar()
        self.where_condition2 = tk.StringVar()
        
        # 查询对比参数
        self.query1 = tk.StringVar()
        self.query2 = tk.StringVar()
        
        # 对比模式：table 或 query
        self.comparison_mode = tk.StringVar(value="table")
        
        # 其他参数
        self.csv_report = tk.StringVar()
        
        # 绑定变量追踪事件
        self.setup_variable_tracing()
        
    def setup_variable_tracing(self):
        """设置变量追踪，实现源数据库配置同步到目标数据库配置"""
        # 绑定源数据库配置变量变化事件
        self.source_db_type.trace_add("write", self.on_source_db_type_change)
        self.source_db_path.trace_add("write", self.on_source_config_change)
        self.source_host.trace_add("write", self.on_source_config_change)
        self.source_port.trace_add("write", self.on_source_config_change)
        self.source_user.trace_add("write", self.on_source_config_change)
        self.source_password.trace_add("write", self.on_source_config_change)
        self.source_database.trace_add("write", self.on_source_config_change)
        
    def on_source_db_type_change(self, *args):
        """源数据库类型变化时的处理"""
        if self.syncing_configs:
            return
            
        # 同步数据库类型
        self.target_db_type.set(self.source_db_type.get())
        
    def on_source_config_change(self, *args):
        """源数据库配置变化时的处理"""
        if self.syncing_configs:
            return
            
        # 同步其他配置参数
        self.syncing_configs = True
        self.target_db_path.set(self.source_db_path.get())
        self.target_host.set(self.source_host.get())
        self.target_port.set(self.source_port.get())
        self.target_user.set(self.source_user.get())
        self.target_password.set(self.source_password.get())
        self.target_database.set(self.source_database.get())
        self.syncing_configs = False
        
    def create_widgets(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 配置网格权重
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(0, weight=1)  # Notebook权重
        main_frame.rowconfigure(1, weight=0)  # 按钮框架权重（不扩展）
        main_frame.rowconfigure(2, weight=2)  # 结果区域权重
        main_frame.rowconfigure(3, weight=0)  # 进度条权重（不扩展）
        
        # 创建Notebook用于分隔不同部分
        notebook = ttk.Notebook(main_frame)
        notebook.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 源数据库配置标签页
        source_frame = ttk.Frame(notebook, padding="10")
        notebook.add(source_frame, text="源数据库配置")
        self.create_database_config_frame(source_frame, "source")
        
        # 目标数据库配置标签页
        target_frame = ttk.Frame(notebook, padding="10")
        notebook.add(target_frame, text="目标数据库配置")
        self.create_database_config_frame(target_frame, "target")
        
        # 对比参数标签页
        params_frame = ttk.Frame(notebook, padding="10")
        notebook.add(params_frame, text="表对比参数")
        self.create_params_frame(params_frame)
        
        # 查询对比标签页
        query_frame = ttk.Frame(notebook, padding="10")
        notebook.add(query_frame, text="查询对比")
        self.create_query_frame(query_frame)
        
        # 控制按钮
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=1, column=0, columnspan=2, pady=(0, 10))
        
        # 添加对比模式选择
        ttk.Label(button_frame, text="对比模式:").pack(side=tk.LEFT, padx=(0, 10))
        mode_frame = ttk.Frame(button_frame)
        mode_frame.pack(side=tk.LEFT, padx=(0, 10))
        
        ttk.Radiobutton(mode_frame, text="表对比", variable=self.comparison_mode, 
                       value="table").pack(side=tk.LEFT)
        ttk.Radiobutton(mode_frame, text="查询对比", variable=self.comparison_mode, 
                       value="query").pack(side=tk.LEFT, padx=(10, 0))
        
        self.run_button = ttk.Button(button_frame, text="开始对比", command=self.run_comparison)
        self.run_button.pack(side=tk.LEFT, padx=(10, 10))
        
        self.save_button = ttk.Button(button_frame, text="保存配置", command=self.save_config)
        self.save_button.pack(side=tk.LEFT, padx=(0, 10))
        
        self.load_button = ttk.Button(button_frame, text="加载配置", command=self.load_config)
        self.load_button.pack(side=tk.LEFT, padx=(0, 10))
        
        # 结果显示区域
        result_frame = ttk.LabelFrame(main_frame, text="对比结果", padding="10")
        result_frame.grid(row=2, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)
        
        self.result_text = scrolledtext.ScrolledText(result_frame, height=15)
        self.result_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 进度条
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 初始化界面状态
        self.root.after(100, self.initialize_ui_state)
        
    def initialize_ui_state(self):
        """初始化界面状态"""
        self.on_db_type_change("source")
        self.on_db_type_change("target")
        
    def create_database_config_frame(self, parent, db_type_prefix):
        # 数据库类型选择
        ttk.Label(parent, text="数据库类型:").grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        db_type_combo = ttk.Combobox(parent, textvariable=getattr(self, f"{db_type_prefix}_db_type"), 
                                    values=["sqlite", "mysql", "postgresql"], state="readonly")
        db_type_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(5, 0))
        db_type_combo.bind('<<ComboboxSelected>>', lambda e: self.on_db_type_change(db_type_prefix))
        
        # SQLite配置
        sqlite_frame = ttk.Frame(parent)
        sqlite_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        sqlite_frame.columnconfigure(1, weight=1)
        
        ttk.Label(sqlite_frame, text="数据库文件:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(sqlite_frame, textvariable=getattr(self, f"{db_type_prefix}_db_path")).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=(5, 5))
        ttk.Button(sqlite_frame, text="浏览", command=lambda: self.browse_file(db_type_prefix)).grid(
            row=0, column=2, sticky=tk.W)
        
        # MySQL/PostgreSQL配置
        server_frame = ttk.Frame(parent)
        server_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10))
        server_frame.columnconfigure(1, weight=1)
        
        ttk.Label(server_frame, text="主机地址:").grid(row=0, column=0, sticky=tk.W)
        ttk.Entry(server_frame, textvariable=getattr(self, f"{db_type_prefix}_host")).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=(5, 5), pady=(0, 5))
        
        ttk.Label(server_frame, text="端口:").grid(row=0, column=2, sticky=tk.W)
        ttk.Entry(server_frame, textvariable=getattr(self, f"{db_type_prefix}_port"), width=10).grid(
            row=0, column=3, sticky=tk.W, padx=(5, 5), pady=(0, 5))
        
        ttk.Label(server_frame, text="用户名:").grid(row=1, column=0, sticky=tk.W)
        ttk.Entry(server_frame, textvariable=getattr(self, f"{db_type_prefix}_user")).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(5, 5), pady=(0, 5))
        
        ttk.Label(server_frame, text="密码:").grid(row=1, column=2, sticky=tk.W)
        ttk.Entry(server_frame, textvariable=getattr(self, f"{db_type_prefix}_password"), show="*").grid(
            row=1, column=3, sticky=(tk.W, tk.E), padx=(5, 5), pady=(0, 5))
        
        ttk.Label(server_frame, text="数据库名:").grid(row=2, column=0, sticky=tk.W)
        ttk.Entry(server_frame, textvariable=getattr(self, f"{db_type_prefix}_database")).grid(
            row=2, column=1, sticky=(tk.W, tk.E), padx=(5, 5))
        
        # 保存frame引用以便动态显示/隐藏
        setattr(self, f"{db_type_prefix}_sqlite_frame", sqlite_frame)
        setattr(self, f"{db_type_prefix}_server_frame", server_frame)
        
    def create_params_frame(self, parent):
        parent.columnconfigure(1, weight=1)
        
        # 表名
        ttk.Label(parent, text="表1名称:").grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.table1).grid(row=0, column=1, sticky=(tk.W, tk.E), 
                                                         padx=(5, 0), pady=(0, 5))
        
        ttk.Label(parent, text="表2名称:").grid(row=1, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.table2).grid(row=1, column=1, sticky=(tk.W, tk.E), 
                                                         padx=(5, 0), pady=(0, 5))
        
        # 字段
        ttk.Label(parent, text="指定字段:").grid(row=2, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.fields).grid(row=2, column=1, sticky=(tk.W, tk.E), 
                                                         padx=(5, 0), pady=(0, 5))
        ttk.Label(parent, text="多个字段用逗号分隔").grid(row=2, column=2, sticky=tk.W, padx=(5, 0))
        
        # 排除字段
        ttk.Label(parent, text="排除字段:").grid(row=3, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.exclude).grid(row=3, column=1, sticky=(tk.W, tk.E), 
                                                          padx=(5, 0), pady=(0, 5))
        ttk.Label(parent, text="多个字段用逗号分隔").grid(row=3, column=2, sticky=tk.W, padx=(5, 0))
        
        # WHERE条件
        ttk.Label(parent, text="通用WHERE条件:").grid(row=4, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.where_condition).grid(row=4, column=1, columnspan=2, 
                                                                  sticky=(tk.W, tk.E), padx=(5, 0), pady=(0, 5))
        
        # 表1的WHERE条件
        ttk.Label(parent, text="表1 WHERE条件:").grid(row=5, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.where_condition1).grid(row=5, column=1, columnspan=2, 
                                                                   sticky=(tk.W, tk.E), padx=(5, 0), pady=(0, 5))
        
        # 表2的WHERE条件
        ttk.Label(parent, text="表2 WHERE条件:").grid(row=6, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.where_condition2).grid(row=6, column=1, columnspan=2, 
                                                                   sticky=(tk.W, tk.E), padx=(5, 0), pady=(0, 5))
        
        # CSV报告
        ttk.Label(parent, text="CSV报告:").grid(row=7, column=0, sticky=tk.W, pady=(0, 5))
        ttk.Entry(parent, textvariable=self.csv_report).grid(row=7, column=1, sticky=(tk.W, tk.E), 
                                                             padx=(5, 0), pady=(0, 5))
        ttk.Button(parent, text="浏览", command=self.browse_csv_file).grid(row=7, column=2, sticky=tk.W, 
                                                                           padx=(5, 0))
        
    def create_query_frame(self, parent):
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        parent.rowconfigure(3, weight=1)
        
        # 查询1
        ttk.Label(parent, text="查询1 SQL:").grid(row=0, column=0, sticky=(tk.W, tk.N), pady=(0, 5))
        self.query1_text = scrolledtext.ScrolledText(parent, height=8)
        self.query1_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 查询2
        ttk.Label(parent, text="查询2 SQL:").grid(row=2, column=0, sticky=(tk.W, tk.N), pady=(0, 5))
        self.query2_text = scrolledtext.ScrolledText(parent, height=8)
        self.query2_text.grid(row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        # 绑定变量到文本框
        self.query1_text.bind('<KeyPress>', self.on_query1_change)
        self.query2_text.bind('<KeyPress>', self.on_query2_change)
        
    def on_query1_change(self, event=None):
        self.root.after(10, lambda: self.query1.set(self.query1_text.get("1.0", tk.END).strip()))
        
    def on_query2_change(self, event=None):
        self.root.after(10, lambda: self.query2.set(self.query2_text.get("1.0", tk.END).strip()))
        
    def setup_styles(self):
        style = ttk.Style()
        style.configure("TButton", padding=6)
        style.configure("TLabel", padding=3)
        style.configure("TEntry", padding=3)
        
    def on_db_type_change(self, db_type_prefix):
        db_type = getattr(self, f"{db_type_prefix}_db_type").get()
        sqlite_frame = getattr(self, f"{db_type_prefix}_sqlite_frame")
        server_frame = getattr(self, f"{db_type_prefix}_server_frame")
        
        # 根据数据库类型显示/隐藏相应配置项
        if db_type == "sqlite":
            sqlite_frame.grid()
            server_frame.grid_remove()
        else:
            sqlite_frame.grid_remove()
            server_frame.grid()
            # 只有当端口为空时才设置默认端口
            port_var = getattr(self, f"{db_type_prefix}_port")
            if not port_var.get():
                if db_type == "mysql":
                    port_var.set("3306")
                elif db_type == "postgresql":
                    port_var.set("5432")
                
    def browse_file(self, db_type_prefix):
        file_path = filedialog.askopenfilename(
            title="选择数据库文件",
            filetypes=[("SQLite数据库", "*.db *.sqlite *.db3"), ("所有文件", "*.*")]
        )
        if file_path:
            getattr(self, f"{db_type_prefix}_db_path").set(file_path)
            
    def browse_csv_file(self):
        file_path = filedialog.asksaveasfilename(
            title="保存CSV报告",
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")]
        )
        if file_path:
            self.csv_report.set(file_path)
            
    def save_config(self):
        config = {
            "source_db_type": self.source_db_type.get(),
            "source_db_path": self.source_db_path.get(),
            "source_host": self.source_host.get(),
            "source_port": self.source_port.get(),
            "source_user": self.source_user.get(),
            "source_password": self.source_password.get(),
            "source_database": self.source_database.get(),
            "target_db_type": self.target_db_type.get(),
            "target_db_path": self.target_db_path.get(),
            "target_host": self.target_host.get(),
            "target_port": self.target_port.get(),
            "target_user": self.target_user.get(),
            "target_password": self.target_password.get(),
            "target_database": self.target_database.get(),
            "table1": self.table1.get(),
            "table2": self.table2.get(),
            "fields": self.fields.get(),
            "exclude": self.exclude.get(),
            "where_condition": self.where_condition.get(),
            "where_condition1": self.where_condition1.get(),
            "where_condition2": self.where_condition2.get(),
            "query1": self.query1.get(),
            "query2": self.query2.get(),
            "comparison_mode": self.comparison_mode.get(),  # 保存对比模式
            "csv_report": self.csv_report.get()
        }
        
        file_path = filedialog.asksaveasfilename(
            title="保存配置文件",
            defaultextension=".json",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                messagebox.showinfo("成功", "配置已保存")
            except Exception as e:
                messagebox.showerror("错误", f"保存配置失败: {str(e)}")
                
    def load_config(self):
        file_path = filedialog.askopenfilename(
            title="加载配置文件",
            filetypes=[("JSON文件", "*.json"), ("所有文件", "*.*")]
        )
        
        if file_path:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                # 加载配置
                self.source_db_type.set(config.get("source_db_type", "postgresql"))
                self.source_db_path.set(config.get("source_db_path", ""))
                self.source_host.set(config.get("source_host", "localhost"))
                self.source_port.set(config.get("source_port", ""))
                self.source_user.set(config.get("source_user", ""))
                self.source_password.set(config.get("source_password", ""))
                self.source_database.set(config.get("source_database", ""))
                
                self.target_db_type.set(config.get("target_db_type", "postgresql"))
                self.target_db_path.set(config.get("target_db_path", ""))
                self.target_host.set(config.get("target_host", "localhost"))
                self.target_port.set(config.get("target_port", ""))
                self.target_user.set(config.get("target_user", ""))
                self.target_password.set(config.get("target_password", ""))
                self.target_database.set(config.get("target_database", ""))
                
                self.table1.set(config.get("table1", ""))
                self.table2.set(config.get("table2", ""))
                self.fields.set(config.get("fields", ""))
                self.exclude.set(config.get("exclude", ""))
                self.where_condition.set(config.get("where_condition", ""))
                self.where_condition1.set(config.get("where_condition1", ""))
                self.where_condition2.set(config.get("where_condition2", ""))
                
                # 加载查询配置
                self.query1.set(config.get("query1", ""))
                self.query2.set(config.get("query2", ""))
                # 更新文本框内容
                self.query1_text.delete(1.0, tk.END)
                self.query1_text.insert(tk.END, self.query1.get())
                self.query2_text.delete(1.0, tk.END)
                self.query2_text.insert(tk.END, self.query2.get())
                
                # 加载对比模式
                self.comparison_mode.set(config.get("comparison_mode", "table"))
                
                self.csv_report.set(config.get("csv_report", ""))
                
                # 更新界面
                self.on_db_type_change("source")
                self.on_db_type_change("target")
                
                messagebox.showinfo("成功", "配置已加载")
            except Exception as e:
                messagebox.showerror("错误", f"加载配置失败: {str(e)}")
                
    def run_comparison(self):
        # 在后台线程中运行对比，避免界面冻结
        thread = threading.Thread(target=self._run_comparison_thread)
        thread.daemon = True
        thread.start()
        
    def _run_comparison_thread(self):
        # 在主线程中更新UI
        self.root.after(0, self._start_comparison_ui)
        
        try:
            # 根据用户选择的模式判断是表对比还是查询对比
            if self.comparison_mode.get() == "query" and self.query1.get().strip() and self.query2.get().strip():
                # 执行查询对比
                result = self._run_query_comparison()
            elif self.comparison_mode.get() == "table" and self.table1.get().strip() and self.table2.get().strip():
                # 执行表对比
                result = self._run_table_comparison()
            else:
                if self.comparison_mode.get() == "query":
                    raise ValueError("请提供两个查询语句进行查询对比")
                else:
                    raise ValueError("请提供两个表名进行表对比")
            
            # 显示结果
            self.root.after(0, lambda: self._display_result(result))
        except Exception as e:
            self.root.after(0, lambda: self._display_error(str(e)))
            
    def _run_table_comparison(self):
        """执行表对比"""
        # 准备参数
        fields_list = None
        if self.fields.get().strip():
            fields_list = [f.strip() for f in self.fields.get().split(",")]
            
        exclude_list = None
        if self.exclude.get().strip():
            exclude_list = [f.strip() for f in self.exclude.get().split(",")]
            
        result = run_comparison(
            source_db_type=self.source_db_type.get(),
            source_db_path=self.source_db_path.get() if self.source_db_type.get() == "sqlite" else None,
            source_host=self.source_host.get() if self.source_db_type.get() != "sqlite" else None,
            source_port=int(self.source_port.get()) if self.source_port.get() and self.source_db_type.get() != "sqlite" else None,
            source_user=self.source_user.get() if self.source_db_type.get() != "sqlite" else None,
            source_password=self.source_password.get() if self.source_db_type.get() != "sqlite" else None,
            source_database=self.source_database.get() if self.source_db_type.get() != "sqlite" else None,
            target_db_type=self.target_db_type.get(),
            target_db_path=self.target_db_path.get() if self.target_db_type.get() == "sqlite" else None,
            target_host=self.target_host.get() if self.target_db_type.get() != "sqlite" else None,
            target_port=int(self.target_port.get()) if self.target_port.get() and self.target_db_type.get() != "sqlite" else None,
            target_user=self.target_user.get() if self.target_db_type.get() != "sqlite" else None,
            target_password=self.target_password.get() if self.target_db_type.get() != "sqlite" else None,
            target_database=self.target_database.get() if self.target_db_type.get() != "sqlite" else None,
            table1=self.table1.get(),
            table2=self.table2.get(),
            fields=fields_list,
            exclude=exclude_list,
            where=self.where_condition.get(),
            where1=self.where_condition1.get(),
            where2=self.where_condition2.get(),
            csv_report=self.csv_report.get()
        )
        return result
    
    def _run_query_comparison(self):
        """执行查询对比"""
        # 获取数据库适配器
        source_db_adapter = get_database_adapter(self.source_db_type.get())
        target_db_adapter = get_database_adapter(self.target_db_type.get())
        
        # 建立源数据库连接
        if self.source_db_type.get() == 'sqlite':
            if not self.source_db_path.get():
                raise ValueError("SQLite数据库需要指定数据库文件路径")
            source_db_adapter.connect(db_path=self.source_db_path.get())
        else:
            if not all([self.source_host.get(), self.source_user.get(), self.source_password.get(), self.source_database.get()]):
                raise ValueError("MySQL和PostgreSQL需要指定主机地址、用户名、密码和数据库名")
            connect_params = {
                'host': self.source_host.get(),
                'user': self.source_user.get(),
                'password': self.source_password.get(),
                'database': self.source_database.get()
            }
            if self.source_port.get():
                connect_params['port'] = int(self.source_port.get())
            source_db_adapter.connect(**connect_params)

        # 建立目标数据库连接
        if self.target_db_type.get() == 'sqlite':
            if not self.target_db_path.get():
                raise ValueError("SQLite数据库需要指定数据库文件路径")
            target_db_adapter.connect(db_path=self.target_db_path.get())
        else:
            if not all([self.target_host.get(), self.target_user.get(), self.target_password.get(), self.target_database.get()]):
                raise ValueError("MySQL和PostgreSQL需要指定主机地址、用户名、密码和数据库名")
            connect_params = {
                'host': self.target_host.get(),
                'user': self.target_user.get(),
                'password': self.target_password.get(),
                'database': self.target_database.get()
            }
            if self.target_port.get():
                connect_params['port'] = int(self.target_port.get())
            target_db_adapter.connect(**connect_params)
        
        # 创建查询对比器并执行对比
        query_comparator = QueryComparator(source_db_adapter, target_db_adapter)
        result = query_comparator.compare_queries(self.query1.get(), self.query2.get())
        
        # 生成CSV报告
        if self.csv_report.get():
            try:
                query_comparator.generate_csv_report(result, self.csv_report.get())
            except Exception as e:
                # 如果生成CSV报告失败，记录错误但不中断程序
                import logging
                logging.error(f"生成CSV报告失败: {str(e)}", exc_info=True)
        
        # 关闭数据库连接
        source_db_adapter.close()
        target_db_adapter.close()
        
        return result
    
    def _start_comparison_ui(self):
        self.run_button.config(state="disabled")
        self.progress.start()
        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, "正在执行对比，请稍候...\n")
        
    def _display_result(self, result: Dict[str, Any]):
        self.run_button.config(state="normal")
        self.progress.stop()
        
        # 清空结果区域
        self.result_text.delete(1.0, tk.END)
        
        # 显示结果
        if 'differences' in result and result['differences'] and result['differences'][0]['type'] == 'field_mismatch':
            # 字段不匹配的情况
            diff = result['differences'][0]
            self.result_text.insert(tk.END, f"发现差异: {diff['message']}\n\n")
            self.result_text.insert(tk.END, f"查询1 的字段: {', '.join(result['table1_fields'])}\n")
            self.result_text.insert(tk.END, f"查询2 的字段: {', '.join(result['table2_fields'])}\n\n")
            
            if 'details' in diff:
                if diff['details'].get('only_in_table1'):
                    self.result_text.insert(tk.END, f"仅在查询1中存在的字段: {', '.join(diff['details']['only_in_table1'])}\n")
                if diff['details'].get('only_in_table2'):
                    self.result_text.insert(tk.END, f"仅在查询2中存在的字段: {', '.join(diff['details']['only_in_table2'])}\n")
                if diff['details'].get('common_fields'):
                    self.result_text.insert(tk.END, f"两个查询共有的字段: {', '.join(diff['details']['common_fields'])}\n")
        else:
            # 正常对比结果
            if 'query1' in result:
                self.result_text.insert(tk.END, "查询对比结果:\n")
                self.result_text.insert(tk.END, f"查询1: {result['query1']}\n")
                self.result_text.insert(tk.END, f"查询2: {result['query2']}\n\n")
            else:
                self.result_text.insert(tk.END, f"字段列表: {', '.join(result['fields'])}\n")
                self.result_text.insert(tk.END, f"表 {self.table1.get()} 记录数: {result['table1_row_count']}\n")
                self.result_text.insert(tk.END, f"表 {self.table2.get()} 记录数: {result['table2_row_count']}\n\n")
            
            if result['differences']:
                self.result_text.insert(tk.END, "发现差异:\n")
                for diff in result['differences']:
                    if diff['type'] != 'multiple_row_diff':
                        self.result_text.insert(tk.END, f"- {diff['message']}\n")
                        
            # 显示行差异
            if result['row_differences']:
                self.result_text.insert(tk.END, "\n详细行差异:\n")
                for row_diff in result['row_differences']:
                    self.result_text.insert(tk.END, f"第 {row_diff['row_number']} 行:\n")
                    for diff in row_diff['differences']:
                        table1_label = "查询1" if 'query1' in result else self.table1.get()
                        table2_label = "查询2" if 'query1' in result else self.table2.get()
                        self.result_text.insert(tk.END, f"  字段 '{diff['field']}': {table1_label}={diff['table1_value']}, {table2_label}={diff['table2_value']}\n")
                        
                # 显示总差异数
                multiple_diff_info = [diff for diff in result['differences'] if diff['type'] == 'multiple_row_diff']
                if multiple_diff_info:
                    self.result_text.insert(tk.END, f"\n总共 {multiple_diff_info[0]['count']} 行存在数据差异\n")
            elif not result['differences']:
                self.result_text.insert(tk.END, "未发现明显差异\n")
                
            # CSV报告提示
            if self.csv_report.get():
                self.result_text.insert(tk.END, f"\n已生成CSV详细差异报告到: {self.csv_report.get()}\n")
                
    def _display_error(self, error_msg: str):
        self.run_button.config(state="normal")
        self.progress.stop()
        self.result_text.delete(1.0, tk.END)
        self.result_text.insert(tk.END, f"执行出错: {error_msg}\n")
        # 同时显示错误弹窗
        messagebox.showerror("执行出错", f"执行出错: {error_msg}")


def main():
    if not TABLE_DIFF_AVAILABLE:
        print("错误: 无法导入 table_diff 模块，请确保 table_diff.py 文件存在于同一目录下")
        sys.exit(1)
        
    root = tk.Tk()
    app = TableDiffGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()