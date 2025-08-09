#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sys
import os

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def create_test_suite():
    """创建测试套件"""
    suite = unittest.TestSuite()
    
    # 切换到tests目录
    original_cwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    
    try:
        # 动态导入所有测试模块
        test_modules = [
            'test_table_diff',
            'test_csv_report',
            'test_exclude_functionality',
            'test_field_inconsistency',
            'test_multiple_diff',
            'test_no_common_fields',
            'test_partial_field_match',
            'test_same_row_count',
            'test_primary_key_comparison',
            'test_mysql_field_mismatch',
            'test_postgresql_ordering',
            'test_postgresql_field_retrieval',
            'test_postgresql_debug',
            'test_cross_database_comparison',
            'test_query_comparison',
            'test_query_comparison_integration',
            'test_cli_simulation',
            'test_gui_query_functionality',
            'test_streaming_comparison',
            'simple_test',
            'comprehensive_test',
            'test_dm_adapter'  # 添加达梦数据库测试模块
        ]
        
        for module_name in test_modules:
            try:
                module = __import__(module_name)
                # 加载模块中的所有测试
                loader = unittest.TestLoader()
                module_suite = loader.loadTestsFromModule(module)
                suite.addTest(module_suite)
            except Exception as e:
                print(f"警告: 无法导入测试模块 {module_name}: {e}")
        
        return suite
    finally:
        # 恢复原来的工作目录
        os.chdir(original_cwd)

def run_tests():
    """运行所有测试"""
    print("开始运行所有测试...")
    print("=" * 50)
    
    # 切换到tests目录
    original_cwd = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    
    try:
        # 创建测试套件
        suite = create_test_suite()
        
        # 运行测试
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        
        # 输出测试结果摘要
        print("\n" + "=" * 50)
        print("测试结果摘要:")
        print(f"运行测试数: {result.testsRun}")
        print(f"失败数: {len(result.failures)}")
        print(f"错误数: {len(result.errors)}")
        print(f"成功率: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.2f}%" if result.testsRun > 0 else "0%")
        
        # 如果有失败或错误，返回非零退出码
        if len(result.failures) > 0 or len(result.errors) > 0:
            print("\n详细错误信息:")
            for failure in result.failures:
                print(f"失败: {failure[0]}")
                print(failure[1])
            for error in result.errors:
                print(f"错误: {error[0]}")
                print(error[1])
            return 1
        else:
            print("\n所有测试通过!")
            return 0
    finally:
        # 恢复原来的工作目录
        os.chdir(original_cwd)

if __name__ == '__main__':
    exit_code = run_tests()
    sys.exit(exit_code)