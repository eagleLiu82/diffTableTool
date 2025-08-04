#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import os
import sys

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 发现并运行所有测试
if __name__ == '__main__':
    # 使用unittest的测试发现机制
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(os.path.abspath(__file__))
    suite = loader.discover(start_dir, pattern='test*.py')
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)