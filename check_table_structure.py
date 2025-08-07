#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector
import sys
import os

# 添加上级目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入MySQL配置
try:
    from mysql_config import MYSQL_CONFIG
except ImportError:
    print("未找到mysql_config.py配置文件，请创建该文件并配置数据库连接参数")
    sys.exit(1)

def check_table_structure():
    """检查表结构"""
    try:
        # 连接到MySQL数据库
        print("连接到MySQL数据库...")
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        print("数据库连接成功")
        
        # 检查employees_2022表结构
        print("\n检查employees_2022表结构:")
        cursor.execute("DESCRIBE employees_2022")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
        # 检查employees_2023表结构
        print("\n检查employees_2023表结构:")
        cursor.execute("DESCRIBE employees_2023")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        
        # 关闭连接
        cursor.close()
        connection.close()
        print("\n检查完成，数据库连接已关闭")
        
    except mysql.connector.Error as e:
        print(f"MySQL错误: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_table_structure()