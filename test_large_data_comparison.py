#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入MySQL配置
try:
    from mysql_config import MYSQL_CONFIG
except ImportError:
    print("未找到mysql_config.py配置文件，请创建该文件并配置数据库连接参数")
    sys.exit(1)

def test_large_data_comparison():
    """测试大数据量表对比"""
    try:
        # 导入表对比工具
        from table_diff import run_comparison
        
        print("开始测试大数据量表对比...")
        print("对比表: employees_2022 和 employees_2023")
        
        # 记录开始时间
        start_time = time.time()
        
        # 执行表对比
        result = run_comparison(
            source_db_type='mysql',
            source_host=MYSQL_CONFIG['host'],
            source_port=MYSQL_CONFIG['port'],
            source_user=MYSQL_CONFIG['user'],
            source_password=MYSQL_CONFIG['password'],
            source_database=MYSQL_CONFIG['database'],
            target_db_type='mysql',
            target_host=MYSQL_CONFIG['host'],
            target_port=MYSQL_CONFIG['port'],
            target_user=MYSQL_CONFIG['user'],
            target_password=MYSQL_CONFIG['password'],
            target_database=MYSQL_CONFIG['database'],
            table1='employees_2022',
            table2='employees_2023',
            fields=['id', 'name', 'email', 'department', 'salary', 'hire_date', 'is_active']
        )
        
        # 记录结束时间
        end_time = time.time()
        
        # 输出结果
        print(f"\n对比完成，耗时: {end_time - start_time:.2f} 秒")
        print(f"表 employees_2022 行数: {result['table1_row_count']}")
        print(f"表 employees_2023 行数: {result['table2_row_count']}")
        
        # 统计差异类型
        row_diff_count = len(result['row_differences'])
        print(f"行差异数量: {row_diff_count}")
        
        # 分类统计差异
        diff_types = {}
        for diff in result['row_differences']:
            diff_type = diff.get('type', 'unknown')
            if diff_type in diff_types:
                diff_types[diff_type] += 1
            else:
                diff_types[diff_type] = 1
        
        print("\n差异类型统计:")
        for diff_type, count in diff_types.items():
            print(f"  {diff_type}: {count}")
        
        # 显示前几个差异示例
        print("\n前5个差异示例:")
        for i, diff in enumerate(result['row_differences'][:5]):
            print(f"  {i+1}. 类型: {diff.get('type', 'unknown')}")
            if 'key' in diff:
                print(f"     主键: {diff['key']}")
            if 'row_number' in diff:
                print(f"     行号: {diff['row_number']}")
            print(f"     差异字段数: {len(diff.get('differences', []))}")
        
        print("\n测试完成!")
        return True
        
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_large_data_comparison()