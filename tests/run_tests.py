#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os

# 添加当前目录和上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from comprehensive_test import run_comprehensive_tests

if __name__ == "__main__":
    print("正在运行测试...")
    try:
        success = run_comprehensive_tests()
        if success:
            print("所有测试通过!")
            sys.exit(0)
        else:
            print("部分测试失败!")
            sys.exit(1)
    except Exception as e:
        print(f"运行测试时出错: {e}")
        sys.exit(1)