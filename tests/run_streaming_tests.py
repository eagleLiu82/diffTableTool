#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import os
import sys

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(__file__))

# 只导入我们新添加的流式处理测试
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
        import traceback
        traceback.print_exc()
        return False

def main():
    """运行流式处理相关的测试"""
    print("开始运行流式处理相关测试套件...")
    
    # 只运行流式处理测试
    test_functions = [
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
    print("流式处理测试套件运行总结")
    print('='*60)
    print(f"总测试套件数: {total_suites}")
    print(f"成功套件数: {total_suites - failed_suites}")
    print(f"失败套件数: {failed_suites}")
    
    if failed_suites == 0:
        print("\n所有流式处理测试套件都成功通过!")
        return True
    else:
        print(f"\n有 {failed_suites} 个流式处理测试套件失败!")
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)