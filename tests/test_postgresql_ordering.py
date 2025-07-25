#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import os
import sys
import tempfile
from unittest.mock import Mock, patch

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from table_diff import (
    TableComparator, 
    SQLiteAdapter, 
    PostgreSQLAdapter,
    MySQLAdapter
)

def test_postgresql_query_building():
    """测试PostgreSQL查询构建时是否添加了排序"""
    print("测试PostgreSQL查询构建时是否添加了排序...")
    
    # 创建Mock的PostgreSQL适配器
    mock_pg_adapter = Mock(spec=PostgreSQLAdapter)
    mock_pg_adapter.get_primary_keys.return_value = ['id']
    
    # 创建对比器
    comparator = TableComparator(mock_pg_adapter)
    comparator.set_tables('table1', 'table2')
    
    # 构建查询
    fields = ['name', 'email', 'age']
    query = comparator.build_query(fields, 'table1')
    
    print(f"构建的查询: {query}")
    
    # 验证查询中是否包含ORDER BY子句
    if 'ORDER BY' in query:
        print("✅ PostgreSQL查询正确包含了ORDER BY子句")
        return True
    else:
        print("❌ PostgreSQL查询未包含ORDER BY子句")
        return False

def test_mysql_query_building():
    """测试MySQL查询构建时是否添加了排序"""
    print("\n测试MySQL查询构建时是否添加了排序...")
    
    # 创建Mock的MySQL适配器
    mock_mysql_adapter = Mock(spec=MySQLAdapter)
    mock_mysql_adapter.get_primary_keys.return_value = ['id']
    
    # 创建对比器
    comparator = TableComparator(mock_mysql_adapter)
    comparator.set_tables('table1', 'table2')
    
    # 构建查询
    fields = ['name', 'email', 'age']
    query = comparator.build_query(fields, 'table1')
    
    print(f"构建的查询: {query}")
    
    # 验证查询中是否包含ORDER BY子句
    if 'ORDER BY' in query:
        print("✅ MySQL查询正确包含了ORDER BY子句")
        return True
    else:
        print("❌ MySQL查询未包含ORDER BY子句")
        return False

def test_sqlite_query_building():
    """测试SQLite查询构建时是否添加了排序"""
    print("\n测试SQLite查询构建时是否添加了排序...")
    
    # 创建Mock的SQLite适配器
    mock_sqlite_adapter = Mock(spec=SQLiteAdapter)
    mock_sqlite_adapter.get_primary_keys.return_value = ['id']
    
    # 创建对比器
    comparator = TableComparator(mock_sqlite_adapter)
    comparator.set_tables('table1', 'table2')
    
    # 构建查询
    fields = ['name', 'email', 'age']
    query = comparator.build_query(fields, 'table1')
    
    print(f"构建的查询: {query}")
    
    # 验证查询中是否包含ORDER BY子句
    if 'ORDER BY' in query:
        print("✅ SQLite查询正确包含了ORDER BY子句")
        return True
    else:
        print("❌ SQLite查询未包含ORDER BY子句")
        return False

def test_postgresql_no_primary_key():
    """测试PostgreSQL表没有主键时的查询构建"""
    print("\n测试PostgreSQL表没有主键时的查询构建...")
    
    # 创建Mock的PostgreSQL适配器
    mock_pg_adapter = Mock(spec=PostgreSQLAdapter)
    mock_pg_adapter.get_primary_keys.return_value = []  # 没有主键
    
    # 创建对比器
    comparator = TableComparator(mock_pg_adapter)
    comparator.set_tables('table1', 'table2')
    
    # 构建查询
    fields = ['name', 'email', 'age']
    query = comparator.build_query(fields, 'table1')
    
    print(f"构建的查询: {query}")
    
    # 验证查询中是否包含ORDER BY子句（应该使用所有字段排序）
    if 'ORDER BY' in query and 'name, email, age' in query:
        print("✅ PostgreSQL无主键时查询正确使用了所有字段排序")
        return True
    else:
        print("❌ PostgreSQL无主键时查询未正确排序")
        return False

def test_with_where_condition():
    """测试带WHERE条件时的查询构建"""
    print("\n测试带WHERE条件时的查询构建...")
    
    # 创建Mock的PostgreSQL适配器
    mock_pg_adapter = Mock(spec=PostgreSQLAdapter)
    mock_pg_adapter.get_primary_keys.return_value = ['id']
    
    # 创建对比器
    comparator = TableComparator(mock_pg_adapter)
    comparator.set_tables('table1', 'table2')
    comparator.set_where_condition("age > 18")
    
    # 构建查询
    fields = ['name', 'email', 'age']
    query = comparator.build_query(fields, 'table1')
    
    print(f"构建的查询: {query}")
    
    # 验证查询是否同时包含WHERE和ORDER BY子句
    if 'WHERE age > 18' in query and 'ORDER BY id' in query:
        print("✅ 带WHERE条件的查询正确包含了WHERE和ORDER BY子句")
        return True
    else:
        print("❌ 带WHERE条件的查询未正确构建")
        return False

if __name__ == "__main__":
    print("测试PostgreSQL排序功能")
    print("=" * 30)
    
    success1 = test_postgresql_query_building()
    success2 = test_mysql_query_building()
    success3 = test_sqlite_query_building()
    success4 = test_postgresql_no_primary_key()
    success5 = test_with_where_condition()
    
    if all([success1, success2, success3, success4, success5]):
        print("\n🎉 所有PostgreSQL排序测试通过!")
        sys.exit(0)
    else:
        print("\n❌ 部分PostgreSQL排序测试失败!")
        sys.exit(1)