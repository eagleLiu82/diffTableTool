import sqlite3

def create_test_database():
    # 连接到数据库（如果不存在会自动创建）
    conn = sqlite3.connect('test_manual.db')
    
    # 创建2022年员工表
    conn.execute('''
        CREATE TABLE employees_2022 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            department TEXT,
            salary REAL,
            hire_date TEXT,
            is_active BOOLEAN
        )
    ''')
    
    # 创建2023年员工表（结构略有不同）
    conn.execute('''
        CREATE TABLE employees_2023 (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            department TEXT,
            salary REAL,
            hire_date TEXT,
            phone TEXT,
            is_active BOOLEAN,
            last_login TEXT
        )
    ''')
    
    # 插入测试数据
    conn.execute('''
        INSERT INTO employees_2022 (id, name, email, department, salary, hire_date, is_active)
        VALUES (1, '张三', 'zhangsan@company.com', '技术部', 15000.00, '2021-03-15', 1)
    ''')
    
    conn.execute('''
        INSERT INTO employees_2023 (id, name, email, department, salary, hire_date, phone, is_active, last_login)
        VALUES (1, '张三', 'zhangsan@company.com', '技术部', 16000.00, '2021-03-15', '13800138000', 1, '2023-06-01 09:00:00')
    ''')
    
    # 提交更改并关闭连接
    conn.commit()
    conn.close()
    print("测试数据库创建完成")

if __name__ == "__main__":
    create_test_database()