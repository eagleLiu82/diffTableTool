import sqlite3

def create_multiple_diff_test_db():
    """创建多行差异的测试数据库"""
    conn = sqlite3.connect('multiple_diff_test.db')
    
    # 创建表1
    conn.execute('''
        CREATE TABLE products_old (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            category TEXT
        )
    ''')
    
    # 创建表2
    conn.execute('''
        CREATE TABLE products_new (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            category TEXT
        )
    ''')
    
    # 插入多行有差异的数据
    old_data = [
        (1, '笔记本电脑', 5999.99, '电子产品'),
        (2, '机械键盘', 299.99, '电子产品'),
        (3, '办公椅', 899.99, '办公用品'),
        (4, '显示器', 1299.99, '电子产品')
    ]
    
    new_data = [
        (1, '笔记本电脑', 5999.99, '电子产品'),  # 相同
        (2, '机械键盘', 399.99, '外设产品'),    # 价格和类别不同
        (3, '办公椅', 999.99, '家具'),         # 价格和类别不同
        (4, '显示器', 1399.99, '电子产品')      # 价格不同
    ]
    
    conn.executemany("INSERT INTO products_old VALUES (?, ?, ?, ?)", old_data)
    conn.executemany("INSERT INTO products_new VALUES (?, ?, ?, ?)", new_data)
    
    conn.commit()
    conn.close()
    print("多行差异测试数据库创建完成: multiple_diff_test.db")

if __name__ == "__main__":
    create_multiple_diff_test_db()