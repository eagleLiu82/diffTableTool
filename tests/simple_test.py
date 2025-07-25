#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import tempfile
import os
import sys

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import TableComparator, SQLiteAdapter, create_sample_database

def test_basic_functionality():
    """测试基本功能"""
    print("开始测试基本功能...")
    
    try:
        print("✓ 成功导入模块")
        
        # 测试创建临时数据库
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        db_path = temp_db.name
        print(f"✓ 创建临时数据库文件: {db_path}")
        
        # 测试创建示例数据库
        create_sample_database(db_path)
        print("✓ 成功创建示例数据库")
        
        # 测试数据库适配器
        adapter = SQLiteAdapter()
        connection = adapter.connect(db_path=db_path)
        print("✓ 成功连接到数据库")
        
        # 测试获取表字段
        fields_old = adapter.get_table_fields('users_old')
        fields_new = adapter.get_table_fields('users_new')
        print(f"✓ users_old 表字段: {fields_old}")
        print(f"✓ users_new 表字段: {fields_new}")
        
        # 测试表对比器
        comparator = TableComparator(adapter)
        comparator.set_tables('users_old', 'users_new')
        result = comparator.compare()
        print("✓ 成功执行表对比")
        print(f"✓ 对比字段: {result['fields']}")
        print(f"✓ users_old 行数: {result['table1_row_count']}")
        print(f"✓ users_new 行数: {result['table2_row_count']}")
        
        # 关闭连接并清理
        adapter.close()
        os.unlink(db_path)
        print("✓ 成功关闭数据库并清理文件")
        
        print("\n所有测试通过!")
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("数据库表对比工具简单测试")
    print("=" * 30)
    
    success = test_basic_functionality()
    
    if success:
        print("\n🎉 简单测试完成，基本功能正常!")
        sys.exit(0)
    else:
        print("\n❌ 简单测试失败，请检查代码!")
        sys.exit(1)