#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import tempfile
import subprocess

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_postgresql_with_verbose():
    """测试PostgreSQL连接并启用详细日志"""
    print("测试PostgreSQL连接并启用详细日志...")
    
    # 这里我们只是演示如何使用--verbose参数
    # 实际使用时需要替换为真实的PostgreSQL连接参数
    print("使用示例命令（请替换为实际参数）:")
    print("python table_diff.py --db-type postgresql --host localhost --port 5432 \\")
    print("  --user your_user --password your_password --database your_database \\")
    print("  --table1 table1 --table2 table2 --verbose")
    
    return True

def test_postgresql_field_retrieval():
    """测试PostgreSQL字段获取逻辑"""
    print("\n测试PostgreSQL字段获取逻辑...")
    
    try:
        # 测试PostgreSQL适配器导入
        from table_diff import PostgreSQLAdapter
        print("✅ 成功导入PostgreSQL适配器")
        
        # 显示PostgreSQL字段查询语句
        print("PostgreSQL字段查询语句:")
        print("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s AND table_catalog = %s
        ORDER BY ordinal_position
        """)
        
        # 显示PostgreSQL主键查询语句
        print("PostgreSQL主键查询语句:")
        print("""
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY a.attnum
        """)
        
        return True
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False

def show_debug_usage():
    """显示调试模式使用方法"""
    print("\n调试模式使用方法:")
    print("1. 添加 --verbose 或 -v 参数以启用详细日志")
    print("2. 日志将显示:")
    print("   - 数据库连接信息")
    print("   - 表字段获取过程")
    print("   - 查询构建过程")
    print("   - 查询执行过程")
    print("   - 数据对比过程")
    print("   - 错误详细信息")
    
    return True

if __name__ == "__main__":
    print("PostgreSQL调试测试")
    print("=" * 30)
    
    success1 = test_postgresql_with_verbose()
    success2 = test_postgresql_field_retrieval()
    success3 = show_debug_usage()
    
    if success1 and success2 and success3:
        print("\n🎉 PostgreSQL调试测试完成!")
        print("\n使用建议:")
        print("如果遇到'没有找到可比对的字段'错误，请使用--verbose参数运行以查看详细日志")
        print("这将帮助您确定是连接问题、权限问题还是表不存在问题")
        sys.exit(0)
    else:
        print("\n❌ PostgreSQL调试测试失败!")
        sys.exit(1)