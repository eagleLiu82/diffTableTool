#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3

def create_mismatch_database():
    """创建字段不匹配的测试数据库"""
    conn = sqlite3.connect('field_mismatch_test.db')
    
    # 创建两个完全没有公共字段的表
    conn.execute('''
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            price REAL,
            category TEXT
        )
    ''')
    
    conn.execute('''
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            phone TEXT
        )
    ''')
    
    # 插入测试数据
    conn.execute("INSERT INTO products (product_name, price, category) VALUES (?, ?, ?)", 
                 ("笔记本电脑", 5999.99, "电子产品"))
    conn.execute("INSERT INTO products (product_name, price, category) VALUES (?, ?, ?)", 
                 ("机械键盘", 299.99, "电子产品"))
    
    conn.execute("INSERT INTO customers (customer_name, email, phone) VALUES (?, ?, ?)", 
                 ("张三", "zhangsan@example.com", "13800138000"))
    conn.execute("INSERT INTO customers (customer_name, email, phone) VALUES (?, ?, ?)", 
                 ("李四", "lisi@example.com", "13800138001"))
    
    conn.commit()
    conn.close()
    print("字段不匹配测试数据库创建完成: field_mismatch_test.db")
    print("\n表结构:")
    print("- products 表字段: product_id, product_name, price, category")
    print("- customers 表字段: customer_id, customer_name, email, phone")
    print("\n这两个表没有公共字段，用于测试字段不匹配的情况。")

if __name__ == "__main__":
    create_mismatch_database()