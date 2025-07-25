#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import TableComparator, SQLiteAdapter

def test_field_mismatch():
    """测试字段不匹配的情况"""
    print("测试字段不匹配的情况...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表
        conn = sqlite3.connect(db_path)
        
        # 创建两个完全没有公共字段的表
        conn.execute('''
            CREATE TABLE table_a (
                id_a INTEGER PRIMARY KEY,
                name_a TEXT NOT NULL,
                value_a INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_b (
                id_b INTEGER PRIMARY KEY,
                name_b TEXT NOT NULL,
                value_b INTEGER
            )
        ''')
        
        # 插入测试数据
        conn.execute("INSERT INTO table_a (name_a, value_a) VALUES (?, ?)", ("Item1", 100))
        conn.execute("INSERT INTO table_b (name_b, value_b) VALUES (?, ?)", ("Item2", 200))
        
        conn.commit()
        conn.close()
        
        # 测试表对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('table_a', 'table_b')
        
        try:
            result = comparator.compare()
            print("❌ 应该抛出异常但没有抛出")
            return False
        except RuntimeError as e:
            print(f"✅ 正确捕获异常: {e}")
            # 验证错误信息中包含表字段信息
            if "没有公共字段" in str(e) and "table_a" in str(e) and "table_b" in str(e):
                print("✅ 错误信息包含表字段信息")
                return True
            else:
                print("❌ 错误信息不完整")
                return False
        except Exception as e:
            print(f"❌ 捕获到意外异常: {e}")
            return False
        finally:
            adapter.close()
            
    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_field_mismatch_with_specified_fields():
    """测试指定字段时即使表字段不匹配也能正常工作"""
    print("\n测试指定字段时的处理...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表
        conn = sqlite3.connect(db_path)
        
        # 创建两个完全没有公共字段的表
        conn.execute('''
            CREATE TABLE table_x (
                id_x INTEGER PRIMARY KEY,
                name_x TEXT NOT NULL,
                value_x INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_y (
                id_y INTEGER PRIMARY KEY,
                name_y TEXT NOT NULL,
                value_y INTEGER
            )
        ''')
        
        # 插入测试数据
        conn.execute("INSERT INTO table_x (name_x, value_x) VALUES (?, ?)", ("Item1", 100))
        conn.execute("INSERT INTO table_y (name_y, value_y) VALUES (?, ?)", ("Item2", 200))
        
        conn.commit()
        conn.close()
        
        # 测试表对比器 - 指定字段（指定在两个表中都存在的字段）
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('table_x', 'table_y')
        # 指定要对比的字段 - 使用两个表中都存在的字段名称（虽然实际不存在，但用于测试指定字段的情况）
        comparator.set_fields(['name'])
        
        try:
            result = comparator.compare()
            print("❌ 应该抛出异常但没有抛出（因为指定的字段在两个表中都不存在）")
            return False
        except RuntimeError as e:
            print(f"✅ 正确捕获异常: {e}")
            return True
        except Exception as e:
            print(f"❌ 捕获到意外异常: {e}")
            return False
        finally:
            adapter.close()
            
    finally:
        # 清理临时文件
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    print("测试字段不匹配处理")
    print("=" * 30)
    
    success1 = test_field_mismatch()
    success2 = test_field_mismatch_with_specified_fields()
    
    if success1 and success2:
        print("\n🎉 所有测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败!")
        sys.exit(1)