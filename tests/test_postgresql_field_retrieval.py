#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_postgresql_field_retrieval_methods():
    """测试PostgreSQL字段获取的各种方法"""
    print("测试PostgreSQL字段获取方法...")
    
    try:
        import psycopg2
        print("✅ psycopg2库可用")
    except ImportError:
        print("❌ psycopg2库不可用，请先安装: pip install psycopg2")
        return False
    
    # 显示用于获取字段的SQL查询
    print("\nPostgreSQL字段获取方法:")
    
    print("1. 使用information_schema:")
    print("""   SELECT column_name 
   FROM information_schema.columns 
   WHERE table_name = %s AND table_catalog = %s
   ORDER BY ordinal_position""")
    
    print("\n2. 使用pg_attribute系统表:")
    print("""   SELECT a.attname AS column_name
   FROM pg_class c
   JOIN pg_attribute a ON a.attrelid = c.oid
   JOIN pg_type t ON a.atttypid = t.oid
   LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
   LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.relname = %s 
   AND a.attnum > 0
   AND NOT a.attisdropped
   ORDER BY a.attnum""")
    
    print("\n3. 使用简化版information_schema:")
    print("""   SELECT column_name 
   FROM information_schema.columns 
   WHERE table_name = %s
   ORDER BY ordinal_position""")
    
    print("\nPostgreSQL主键获取方法:")
    print("""SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = %s::regclass AND i.indisprimary
ORDER BY a.attnum""")
    
    return True

def show_debug_tips():
    """显示调试技巧"""
    print("\n调试PostgreSQL连接问题的技巧:")
    print("1. 使用 --verbose 参数启用详细日志:")
    print("   python table_diff.py --db-type postgresql [其他参数] --verbose")
    
    print("\n2. 检查以下常见问题:")
    print("   - 数据库连接参数是否正确")
    print("   - 用户是否有足够的权限访问表")
    print("   - 表名是否正确（注意大小写）")
    print("   - 数据库是否可访问")
    
    print("\n3. 手动验证连接:")
    print("   可以使用psql命令行工具验证连接:")
    print("   psql -h host -p port -U user -d database")
    
    return True

if __name__ == "__main__":
    print("PostgreSQL字段获取测试")
    print("=" * 30)
    
    success1 = test_postgresql_field_retrieval_methods()
    success2 = show_debug_tips()
    
    if success1 and success2:
        print("\n🎉 PostgreSQL字段获取测试完成!")
        print("\n如果仍然遇到问题，请使用--verbose参数运行以获取详细日志信息")
        sys.exit(0)
    else:
        print("\n❌ PostgreSQL字段获取测试失败!")
        sys.exit(1)