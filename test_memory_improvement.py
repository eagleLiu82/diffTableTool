#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
import time
import psutil
import gc

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))

# 导入主模块
from table_diff import TableComparator, SQLiteAdapter


def get_memory_usage():
    """获取当前进程的内存使用量(MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def create_large_test_database(db_path, rows=5000):
    """创建大数据量测试用的数据库和表"""
    print(f"创建包含 {rows} 行数据的测试数据库...")
    conn = sqlite3.connect(db_path)
    
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
    
    # 插入大量测试数据
    start_time = time.time()
    for i in range(rows):
        conn.execute(
            "INSERT INTO large_table1 (name, email, age, department) VALUES (?, ?, ?, ?)",
            (f"User{i}", f"user{i}@example.com", 20 + (i % 50), f"Department{i % 10}")
        )
        
        # 在第二个表中插入稍微不同的数据
        conn.execute(
            "INSERT INTO large_table2 (name, email, age, department, phone) VALUES (?, ?, ?, ?, ?)",
            (f"User{i}", f"user{i}@example.com", 20 + (i % 50), f"Department{i % 10}", f"123456789{i % 100}")
        )
        
        # 每1000行提交一次以节省内存
        if i % 1000 == 0:
            conn.commit()
    
    conn.commit()
    conn.close()
    
    end_time = time.time()
    print(f"数据库创建完成，耗时: {end_time - start_time:.2f} 秒")


def test_memory_usage():
    """测试内存使用情况"""
    print("开始测试内存使用情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建大数据量测试数据库
        create_large_test_database(db_path, rows=5000)
        
        # 强制垃圾回收并获取初始内存使用
        gc.collect()
        initial_memory = get_memory_usage()
        print(f"初始内存使用: {initial_memory:.2f} MB")
        
        # 初始化适配器和对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        
        # 设置要比较的表
        comparator.set_tables('large_table1', 'large_table2')
        
        # 执行比较前的内存使用
        gc.collect()
        before_compare_memory = get_memory_usage()
        print(f"比较前内存使用: {before_compare_memory:.2f} MB")
        
        # 执行比较
        start_time = time.time()
        result = comparator.compare()
        end_time = time.time()
        
        # 比较后的内存使用
        gc.collect()
        after_compare_memory = get_memory_usage()
        print(f"比较后内存使用: {after_compare_memory:.2f} MB")
        print(f"内存增加: {after_compare_memory - before_compare_memory:.2f} MB")
        print(f"比较耗时: {end_time - start_time:.2f} 秒")
        
        # 输出结果统计
        print(f"表1行数: {result['table1_row_count']}")
        print(f"表2行数: {result['table2_row_count']}")
        print(f"差异行数: {len(result['row_differences'])}")
        
        # 关闭连接
        adapter.close()
        
        print("内存使用测试完成")
        return True
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        return False
        
    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)


def main():
    """主函数"""
    print("开始测试流式处理功能的内存改进效果...")
    
    try:
        # 检查是否安装了psutil
        import psutil
    except ImportError:
        print("警告: 未安装psutil库，将跳过内存监控")
        print("可以通过运行 'pip install psutil' 安装该库")
        return False
    
    success = test_memory_usage()
    return success


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)