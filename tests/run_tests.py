#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import os
import sys

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(__file__))

# 导入所有测试模块
from test_table_diff import run_all_tests as run_table_diff_tests
from test_csv_report import test_csv_report_generation as run_csv_report_tests
from test_exclude_functionality import run_all_tests as run_exclude_functionality_tests
from test_field_inconsistency import run_all_tests as run_field_inconsistency_tests
from test_multiple_diff import run_all_tests as run_multiple_diff_tests
from test_no_common_fields import run_all_tests as run_no_common_fields_tests
from test_partial_field_match import run_all_tests as run_partial_field_match_tests
from test_query_comparison import run_all_tests as run_query_comparison_tests
from test_same_row_count import run_all_tests as run_same_row_count_tests
from test_cross_database_comparison import run_all_tests as run_cross_database_comparison_tests
from test_query_comparison_integration import run_all_tests as run_query_comparison_integration_tests
from test_cli_simulation import run_all_tests as run_cli_simulation_tests
from test_gui_query_functionality import run_all_tests as run_gui_query_functionality_tests
from test_mysql_field_mismatch import run_all_tests as run_mysql_field_mismatch_tests
from test_postgresql_ordering import run_all_tests as run_postgresql_ordering_tests
from test_postgresql_field_retrieval import run_all_tests as run_postgresql_field_retrieval_tests
from test_postgresql_debug import run_all_tests as run_postgresql_debug_tests
from simple_test import run_all_tests as run_simple_tests
from comprehensive_test import run_all_tests as run_comprehensive_tests
from test_streaming_comparison import run_all_tests as run_streaming_comparison_tests

def run_test_function(test_func, test_name):
    """运行单个测试函数"""
    try:
        print(f"运行测试: {test_name}")
        result = test_func()
        if result:
            print(f"✓ {test_name} 通过")
            return True
        else:
            print(f"✗ {test_name} 失败")
            return False
    except Exception as e:
        print(f"✗ {test_name} 发生异常: {e}")
        return False

def main():
    """运行所有测试"""
    print("开始运行所有测试套件...")
    
    # 收集所有测试函数
    test_functions = [
        (run_table_diff_tests, "Table Diff Tests"),
        (run_csv_report_tests, "CSV Report Tests"),
        (run_exclude_functionality_tests, "Exclude Functionality Tests"),
        (run_field_inconsistency_tests, "Field Inconsistency Tests"),
        (run_multiple_diff_tests, "Multiple Diff Tests"),
        (run_no_common_fields_tests, "No Common Fields Tests"),
        (run_partial_field_match_tests, "Partial Field Match Tests"),
        (run_query_comparison_tests, "Query Comparison Tests"),
        (run_same_row_count_tests, "Same Row Count Tests"),
        (run_cross_database_comparison_tests, "Cross Database Comparison Tests"),
        (run_query_comparison_integration_tests, "Query Comparison Integration Tests"),
        (run_cli_simulation_tests, "CLI Simulation Tests"),
        (run_gui_query_functionality_tests, "GUI Query Functionality Tests"),
        (run_mysql_field_mismatch_tests, "MySQL Field Mismatch Tests"),
        (run_postgresql_ordering_tests, "PostgreSQL Ordering Tests"),
        (run_postgresql_field_retrieval_tests, "PostgreSQL Field Retrieval Tests"),
        (run_postgresql_debug_tests, "PostgreSQL Debug Tests"),
        (run_simple_tests, "Simple Tests"),
        (run_comprehensive_tests, "Comprehensive Tests"),
        (run_streaming_comparison_tests, "Streaming Comparison Tests")
    ]
    
    # 运行所有测试
    failed_suites = 0
    total_suites = len(test_functions)
    
    for i, (test_func, test_name) in enumerate(test_functions, 1):
        print(f"\n{'='*60}")
        print(f"运行测试套件 {i}/{total_suites}: {test_name}")
        print('='*60)
        
        try:
            success = run_test_function(test_func, test_name)
            if not success:
                failed_suites += 1
        except Exception as e:
            print(f"测试套件 {test_name} 发生异常: {e}")
            failed_suites += 1
    
    # 输出总结
    print(f"\n{'='*60}")
    print("测试套件运行总结")
    print('='*60)
    print(f"总测试套件数: {total_suites}")
    print(f"成功套件数: {total_suites - failed_suites}")
    print(f"失败套件数: {failed_suites}")
    
    if failed_suites == 0:
        print("\n所有测试套件都成功通过!")
        return True
    else:
        print(f"\n有 {failed_suites} 个测试套件失败!")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)