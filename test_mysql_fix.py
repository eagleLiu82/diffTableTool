#!/usr/bin/env python3
"""
测试脚本，用于验证MySQL场景的修复
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from table_diff import main
import argparse
import sys


def test_mysql_args():
    """测试MySQL参数解析"""
    # 模拟命令行参数
    test_args = [
        'table_diff.py',
        '--source-db-type', 'mysql',
        '--source-host', 'localhost',
        '--source-port', '3306',
        '--source-user', 'mysqluser',
        '--source-password', 'Laysdbz,.90!',
        '--source-database', 'test_db',
        '--table1', 'employees_2022',
        '--table2', 'employees_2023',
        '--create-sample'  # 添加这个参数避免实际连接数据库
    ]
    
    # 保存原始的sys.argv
    original_argv = sys.argv
    sys.argv = test_args
    
    try:
        # 调用main函数
        main()
        print("MySQL参数解析测试通过")
    except SystemExit as e:
        if e.code == 0:
            print("MySQL参数解析测试通过")
        else:
            print(f"MySQL参数解析测试失败，退出码: {e.code}")
    except Exception as e:
        print(f"MySQL参数解析测试出现异常: {e}")
    finally:
        # 恢复原始的sys.argv
        sys.argv = original_argv


if __name__ == "__main__":
    test_mysql_args()