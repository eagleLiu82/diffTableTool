#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import TableComparator, SQLiteAdapter

def test_no_common_fields_handling():
    """测试没有公共字段时的处理"""
    print("测试没有公共字段时的处理...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（完全没有公共字段）
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE table_a (
                id_a INTEGER PRIMARY KEY,
                name_a TEXT,
                value_a INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_b (
                id_b INTEGER PRIMARY KEY,
                name_b TEXT,
                value_b INTEGER
            )
        ''')
        
        conn.execute("INSERT INTO table_a (name_a, value_a) VALUES (?, ?)", ("Item1", 100))
        conn.execute("INSERT INTO table_b (name_b, value_b) VALUES (?, ?)", ("Item2", 200))
        
        conn.commit()
        conn.close()
        
        # 测试表对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('table_a', 'table_b')
        
        # 执行对比
        result = comparator.compare()
        
        # 验证结果
        print(f"字段列表: {result['fields']}")
        print(f"表table_a行数: {result['table1_row_count']}")
        print(f"表table_b行数: {result['table2_row_count']}")
        print(f"差异信息: {result['differences']}")
        print(f"表table_a字段: {result['table1_fields']}")
        print(f"表table_b字段: {result['table2_fields']}")
        
        # 验证返回了正确的结果
        assert len(result['fields']) == 0, "字段列表应该为空"
        assert len(result['differences']) == 1, "应该有一个差异"
        assert result['differences'][0]['type'] == 'no_common_fields', "差异类型应该是no_common_fields"
        assert len(result['table1_fields']) == 3, "表table_a应该有3个字段"
        assert len(result['table2_fields']) == 3, "表table_b应该有3个字段"
        
        print("✅ 测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
    finally:
        # 清理
        adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_normal_comparison():
    """测试正常对比功能"""
    print("\n测试正常对比功能...")
    
    # 创建临时数据库文件
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    db_path = temp_db.name
    
    try:
        # 创建测试数据库和表（有公共字段）
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE table_x (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                extra_x TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table_y (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                extra_y TEXT
            )
        ''')
        
        conn.execute("INSERT INTO table_x (name, value, extra_x) VALUES (?, ?, ?)", ("Item1", 100, "extra_x"))
        conn.execute("INSERT INTO table_y (name, value, extra_y) VALUES (?, ?, ?)", ("Item1", 200, "extra_y"))
        
        conn.commit()
        conn.close()
        
        # 测试表对比器
        adapter = SQLiteAdapter()
        adapter.connect(db_path=db_path)
        comparator = TableComparator(adapter)
        comparator.set_tables('table_x', 'table_y')
        
        # 执行对比
        result = comparator.compare()
        
        # 验证结果
        print(f"字段列表: {result['fields']}")
        print(f"表table_x行数: {result['table1_row_count']}")
        print(f"表table_y行数: {result['table2_row_count']}")
        print(f"差异信息: {result['differences']}")
        
        # 验证返回了正确的结果
        assert len(result['fields']) == 3, "应该有3个公共字段(id, name, value)"
        assert result['table1_row_count'] == 1, "表table_x应该有1行数据"
        assert result['table2_row_count'] == 1, "表table_y应该有1行数据"
        # 应该有数据差异，因为value字段不同(100 vs 200)
        assert len(result['row_differences']) == 1, "应该有一行数据差异"
        
        print("✅ 测试通过!")
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
    finally:
        # 清理
        adapter.close()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    print("测试没有公共字段的处理")
    print("=" * 30)
    
    success1 = test_no_common_fields_handling()
    success2 = test_normal_comparison()
    
    if success1 and success2:
        print("\n🎉 所有测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分测试失败!")
        sys.exit(1)