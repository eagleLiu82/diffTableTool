#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import os
import sys
import csv

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import TableComparator, SQLiteAdapter, create_sample_database

def test_csv_report_generation():
    """测试CSV报告生成功能"""
    print("开始测试CSV报告生成功能...")
    
    try:
        print("✓ 成功导入模块")
        
        # 创建一个有明确差异的测试数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        print(f"✓ 创建临时数据库文件: {db_path}")
        
        # 手动创建两个有差异的表
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE table1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                email TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                email TEXT
            )
        ''')
        
        # 插入有差异的数据
        # 第一行完全相同
        conn.execute("INSERT INTO table1 (name, age, email) VALUES (?, ?, ?)", ("张三", 25, "zhangsan@example.com"))
        conn.execute("INSERT INTO table2 (name, age, email) VALUES (?, ?, ?)", ("张三", 25, "zhangsan@example.com"))
        
        # 第二行有差异（年龄不同）
        conn.execute("INSERT INTO table1 (name, age, email) VALUES (?, ?, ?)", ("李四", 30, "lisi@example.com"))
        conn.execute("INSERT INTO table2 (name, age, email) VALUES (?, ?, ?)", ("李四", 31, "lisi@example.com"))
        
        # 第三行有多个差异（年龄和邮箱都不同）
        conn.execute("INSERT INTO table1 (name, age, email) VALUES (?, ?, ?)", ("王五", 28, "wangwu@example.com"))
        conn.execute("INSERT INTO table2 (name, age, email) VALUES (?, ?, ?)", ("王五", 29, "wangwu_new@example.com"))
        
        conn.commit()
        conn.close()
        print("✓ 成功创建有差异的测试数据库")
        
        # 测试数据库适配器
        adapter = SQLiteAdapter()
        connection = adapter.connect(db_path=db_path)
        print("✓ 成功连接到数据库")
        
        # 测试表对比器
        comparator = TableComparator(adapter)
        comparator.set_tables('table1', 'table2')
        result = comparator.compare()
        print("✓ 成功执行表对比")
        
        # 检查是否有差异
        row_differences = result.get('row_differences', [])
        print(f"✓ 发现 {len(row_differences)} 行差异")
        
        # 测试CSV报告生成
        csv_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        csv_file.close()
        csv_path = csv_file.name
        print(f"✓ 创建临时CSV文件: {csv_path}")
        
        # 生成CSV报告
        comparator.generate_csv_report(result, csv_path)
        print("✓ 成功生成CSV报告")
        
        # 验证CSV报告内容
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"✓ CSV报告包含 {len(rows)} 行差异数据")
        
        # 验证CSV报告的字段
        expected_fields = ['row_number', 'column_name', 'table1_value', 'table2_value']
        actual_fields = reader.fieldnames
        if set(expected_fields) == set(actual_fields):
            print("✓ CSV报告包含正确的字段")
        else:
            raise AssertionError(f"CSV字段不匹配。期望: {expected_fields}, 实际: {actual_fields}")
        
        # 验证数据行
        if len(rows) > 0:
            print("✓ CSV报告包含差异数据")
            
            # 验证每行数据的结构
            for i, row in enumerate(rows):
                if not all(field in row for field in expected_fields):
                    raise AssertionError(f"第{i+1}行数据缺少字段。行数据: {row}")
            
            print("✓ 所有CSV报告数据行都包含必需字段")
            
            # 验证特定差异
            # 查找第二行年龄差异
            age_diffs = [row for row in rows if row['row_number'] == '2' and row['column_name'] == 'age']
            if age_diffs:
                age_diff = age_diffs[0]
                if age_diff['table1_value'] == '30' and age_diff['table2_value'] == '31':
                    print("✓ 正确识别第二行年龄差异: 30 -> 31")
                else:
                    raise AssertionError(f"第二行年龄差异不正确: {age_diff}")
            else:
                raise AssertionError("未找到第二行年龄差异")
                
            # 查找第三行的差异
            row3_diffs = [row for row in rows if row['row_number'] == '3']
            if len(row3_diffs) >= 2:  # 应该有两个差异（age和email）
                print("✓ 正确识别第三行的多个差异")
            else:
                raise AssertionError(f"第三行应该有至少两个差异，实际找到: {len(row3_diffs)}")
        else:
            raise AssertionError("CSV报告应该包含差异数据，但实际上为空")
        
        # 关闭连接并清理
        adapter.close()
        os.unlink(db_path)
        os.unlink(csv_path)
        print("✓ 成功关闭数据库并清理文件")
        
        print("\nCSV报告功能测试通过!")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_csv_report_with_no_differences():
    """测试没有差异时CSV报告的生成"""
    print("\n开始测试无差异情况下的CSV报告生成功能...")
    
    try:
        print("✓ 成功导入模块")
        
        # 创建一个没有差异的数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        print(f"✓ 创建临时数据库文件: {db_path}")
        
        # 手动创建两个相同的表
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute('''
            CREATE TABLE table1 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE table2 (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
        ''')
        
        # 插入相同的数据
        conn.execute("INSERT INTO table1 (name, value) VALUES (?, ?)", ("test", 123))
        conn.execute("INSERT INTO table2 (name, value) VALUES (?, ?)", ("test", 123))
        conn.commit()
        conn.close()
        print("✓ 成功创建无差异的测试数据库")
        
        # 测试数据库适配器
        adapter = SQLiteAdapter()
        connection = adapter.connect(db_path=db_path)
        print("✓ 成功连接到数据库")
        
        # 测试表对比器
        comparator = TableComparator(adapter)
        comparator.set_tables('table1', 'table2')
        result = comparator.compare()
        print("✓ 成功执行表对比")
        
        # 测试CSV报告生成
        csv_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        csv_file.close()
        csv_path = csv_file.name
        print(f"✓ 创建临时CSV文件: {csv_path}")
        
        # 生成CSV报告
        comparator.generate_csv_report(result, csv_path)
        print("✓ 成功生成CSV报告")
        
        # 验证CSV报告内容
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        print(f"✓ CSV报告包含 {len(rows)} 行差异数据")
        
        # 对于没有差异的情况，应该生成一个只有标题的CSV文件
        if len(rows) == 0:
            print("✓ 无差异情况下CSV报告正确生成（仅包含标题行）")
        else:
            raise AssertionError("预期无差异情况下CSV报告应该为空（除了标题行）")
        
        # 关闭连接并清理
        adapter.close()
        os.unlink(db_path)
        os.unlink(csv_path)
        print("✓ 成功关闭数据库并清理文件")
        
        print("\n无差异情况下的CSV报告功能测试通过!")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("数据库表对比工具CSV报告功能测试")
    print("=" * 40)
    
    success1 = test_csv_report_generation()
    success2 = test_csv_report_with_no_differences()
    
    if success1 and success2:
        print("\n🎉 CSV报告功能测试完成，所有测试通过!")
        sys.exit(0)
    else:
        print("\n❌ CSV报告功能测试失败，请检查代码!")
        sys.exit(1)