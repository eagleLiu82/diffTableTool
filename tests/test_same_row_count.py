import sqlite3

def create_same_row_count_test_db():
    """创建行数相同的测试数据库"""
    conn = sqlite3.connect('same_row_count_test.db')
    
    # 创建表1
    conn.execute('''
        CREATE TABLE employees_old (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            salary INTEGER
        )
    ''')
    
    # 创建表2
    conn.execute('''
        CREATE TABLE employees_new (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT,
            salary INTEGER
        )
    ''')
    
    # 插入相同数量但有差异的数据
    old_data = [
        (1, '张三', '技术部', 10000),
        (2, '李四', '销售部', 8000),
        (3, '王五', '人事部', 6000)
    ]
    
    new_data = [
        (1, '张三', '技术部', 10000),  # 相同
        (2, '李四', '市场部', 9000),   # 部门和薪资不同
        (3, '王五', '人事部', 6000)    # 相同
    ]
    
    conn.executemany("INSERT INTO employees_old VALUES (?, ?, ?, ?)", old_data)
    conn.executemany("INSERT INTO employees_new VALUES (?, ?, ?, ?)", new_data)
    
    conn.commit()
    conn.close()
    print("行数相同的测试数据库创建完成: same_row_count_test.db")

if __name__ == "__main__":
    create_same_row_count_test_db()