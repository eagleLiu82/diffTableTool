#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
from typing import List, Optional, Dict, Any, Union
from abc import ABC, abstractmethod
import logging

# 新增 Union 类型用于 run_comparison 参数类型提示

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseAdapter(ABC):
    """数据库适配器抽象基类"""
    
    @abstractmethod
    def connect(self, **kwargs):
        """建立数据库连接"""
        pass
    
    @abstractmethod
    def get_table_fields(self, table_name: str) -> List[str]:
        """获取表字段列表"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str):
        """执行查询"""
        pass
    
    @abstractmethod
    def close(self):
        """关闭数据库连接"""
        pass
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取表的主键字段列表"""
        return []  # 默认实现，子类可以重写


class SQLiteAdapter(DatabaseAdapter):
    """SQLite数据库适配器"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self, **kwargs):
        db_path = kwargs.get('db_path')
        logger.info(f"连接到SQLite数据库: {db_path}")
        self.connection = sqlite3.connect(db_path)
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取SQLite表 {table_name} 的字段")
        cursor = self.connection.execute(f"PRAGMA table_info({table_name})")
        fields = [row[1] for row in cursor.fetchall()]
        logger.info(f"表 {table_name} 的字段: {fields}")
        return fields
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取SQLite表的主键字段"""
        logger.info(f"获取SQLite表 {table_name} 的主键")
        cursor = self.connection.execute(f"PRAGMA table_info({table_name})")
        primary_keys = [row[1] for row in cursor.fetchall() if row[5] > 0]  # pk列大于0表示是主键
        logger.info(f"表 {table_name} 的主键: {primary_keys}")
        return primary_keys
    
    def execute_query(self, query: str):
        logger.info(f"执行SQLite查询: {query}")
        return self.connection.execute(query)
    
    def close(self):
        if self.connection:
            logger.info("关闭SQLite数据库连接")
            self.connection.close()


class MySQLAdapter(DatabaseAdapter):
    """MySQL数据库适配器"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self, **kwargs):
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("需要安装mysql-connector-python库: pip install mysql-connector-python")
        
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 3306)
        user = kwargs.get('user')
        password = kwargs.get('password')
        database = kwargs.get('database')
        
        logger.info(f"连接到MySQL数据库: {host}:{port}, 用户: {user}, 数据库: {database}")
        
        self.connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取MySQL表 {table_name} 的字段")
        cursor = self.connection.cursor()
        cursor.execute(f"DESCRIBE {table_name}")
        fields = [row[0] for row in cursor.fetchall()]
        logger.info(f"表 {table_name} 的字段: {fields}")
        return fields
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取MySQL表的主键字段"""
        logger.info(f"获取MySQL表 {table_name} 的主键")
        cursor = self.connection.cursor()
        cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
        primary_keys = [row[4] for row in cursor.fetchall()]  # Column_name列
        logger.info(f"表 {table_name} 的主键: {primary_keys}")
        return primary_keys
    
    def execute_query(self, query: str):
        logger.info(f"执行MySQL查询: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
    
    def close(self):
        if self.connection:
            logger.info("关闭MySQL数据库连接")
            self.connection.close()


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL数据库适配器"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self, **kwargs):
        try:
            import psycopg2
        except ImportError:
            raise ImportError("需要安装psycopg2库: pip install psycopg2")
        
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 5432)
        user = kwargs.get('user')
        password = kwargs.get('password')
        database = kwargs.get('database')
        
        logger.info(f"连接到PostgreSQL数据库: {host}:{port}, 用户: {user}, 数据库: {database}")
        
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取PostgreSQL表 {table_name} 的字段")
        cursor = self.connection.cursor()
        
        # 首先尝试使用当前数据库和模式
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_catalog = %s
                ORDER BY ordinal_position
            """, (table_name, self.connection.info.dbname))
            
            fields_result = cursor.fetchall()
            if fields_result:
                fields = [row[0] for row in fields_result]
                logger.info(f"通过information_schema获取到表 {table_name} 的字段: {fields}")
                return fields
        except Exception as e:
            logger.warning(f"通过information_schema获取字段失败: {e}")
        
        # 如果上面的方法失败，尝试直接查询pg_attribute
        try:
            cursor.execute("""
                SELECT a.attname AS column_name
                FROM pg_class c
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON a.atttypid = t.oid
                LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s 
                AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY a.attnum
            """, (table_name,))
            
            fields_result = cursor.fetchall()
            if fields_result:
                fields = [row[0] for row in fields_result]
                logger.info(f"通过pg_attribute获取到表 {table_name} 的字段: {fields}")
                return fields
        except Exception as e:
            logger.warning(f"通过pg_attribute获取字段失败: {e}")
            
        # 如果还是失败，尝试不指定数据库名的方式
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            fields_result = cursor.fetchall()
            fields = [row[0] for row in fields_result]
            logger.info(f"通过information_schema(不指定数据库)获取到表 {table_name} 的字段: {fields}")
            return fields
        except Exception as e:
            logger.error(f"所有尝试获取字段的方法都失败了: {e}")
            
        logger.warning(f"未能获取到表 {table_name} 的任何字段")
        return []
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取PostgreSQL表的主键字段"""
        logger.info(f"获取PostgreSQL表 {table_name} 的主键")
        cursor = self.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
                ORDER BY a.attnum
            """, (table_name,))
            
            primary_keys_result = cursor.fetchall()
            primary_keys = [row[0] for row in primary_keys_result]
            logger.info(f"表 {table_name} 的主键: {primary_keys}")
            return primary_keys
        except Exception as e:
            logger.warning(f"获取主键失败，使用空列表: {e}")
            return []
    
    def execute_query(self, query: str):
        logger.info(f"执行PostgreSQL查询: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
    
    def close(self):
        if self.connection:
            logger.info("关闭PostgreSQL数据库连接")
            self.connection.close()


def get_database_adapter(db_type: str) -> DatabaseAdapter:
    """根据数据库类型获取对应的适配器"""
    logger.info(f"获取数据库适配器: {db_type}")
    adapters = {
        'sqlite': SQLiteAdapter,
        'mysql': MySQLAdapter,
        'postgresql': PostgreSQLAdapter
    }
    
    if db_type not in adapters:
        raise ValueError(f"不支持的数据库类型: {db_type}")
    
    return adapters[db_type]()


class TableComparator:
    """
    数据库表对比工具类
    支持对比同一数据库中的两个表，可以指定字段、排除字段和设置WHERE条件
    """

    def __init__(self, db_adapter: DatabaseAdapter):
        """
        初始化对比工具
        
        :param db_adapter: 数据库适配器实例
        """
        self.db = db_adapter
        self.table1 = None
        self.table2 = None
        self.fields = []
        self.exclude_fields = []
        self.where_condition = None

    def set_tables(self, table1: str, table2: str):
        """
        设置要对比的表
        
        :param table1: 第一个表名
        :param table2: 第二个表名
        """
        logger.info(f"设置对比表: {table1} 和 {table2}")
        self.table1 = table1
        self.table2 = table2

    def set_fields(self, fields: List[str]):
        """
        设置要对比的字段
        
        :param fields: 要对比的字段列表
        """
        logger.info(f"设置对比字段: {fields}")
        self.fields = fields

    def set_exclude_fields(self, exclude_fields: List[str]):
        """
        设置要排除的字段
        
        :param exclude_fields: 要排除的字段列表
        """
        logger.info(f"设置排除字段: {exclude_fields}")
        self.exclude_fields = exclude_fields

    def set_where_condition(self, where_condition: str):
        """
        设置WHERE条件
        
        :param where_condition: WHERE条件字符串
        """
        logger.info(f"设置WHERE条件: {where_condition}")
        self.where_condition = where_condition

    def get_table_fields(self, table_name: str) -> List[str]:
        """
        获取表的所有字段名
        
        :param table_name: 表名
        :return: 字段名列表
        """
        logger.info(f"获取表 {table_name} 的所有字段")
        fields = self.db.get_table_fields(table_name)
        logger.info(f"表 {table_name} 的字段: {fields}")
        return fields

    def get_comparison_fields(self) -> List[str]:
        """
        获取最终要对比的字段列表
        
        :return: 对比字段列表
        """
        logger.info("获取对比字段列表")
        # 如果用户指定了字段，则直接使用
        if self.fields:
            logger.info(f"使用指定的字段: {self.fields}")
            return self.fields

        # 否则获取两个表的公共字段
        logger.info(f"获取表 {self.table1} 的字段")
        fields1 = self.get_table_fields(self.table1)
        logger.info(f"获取表 {self.table2} 的字段")
        fields2 = self.get_table_fields(self.table2)

        # 获取公共字段
        common_fields = list(set(fields1) & set(fields2))
        logger.info(f"两个表的公共字段: {common_fields}")

        # 如果有排除字段，则移除它们
        if self.exclude_fields:
            logger.info(f"排除字段: {self.exclude_fields}")
            common_fields = [f for f in common_fields if f not in self.exclude_fields]
            logger.info(f"排除后剩余字段: {common_fields}")

        logger.info(f"最终对比字段: {common_fields}")
        return common_fields

    def build_query(self, fields: List[str], table_name: str) -> str:
        """
        构建查询SQL
        
        :param fields: 字段列表
        :param table_name: 表名
        :return: 查询SQL语句
        """
        logger.info(f"为表 {table_name} 构建查询，字段: {fields}")
        field_list = ', '.join(fields)
        query = f"SELECT {field_list} FROM {table_name}"
        
        # 添加WHERE条件
        if self.where_condition:
            query += f" WHERE {self.where_condition}"
            logger.info(f"添加WHERE条件: {self.where_condition}")
        
        # 尝试获取主键进行排序
        primary_keys = self.db.get_primary_keys(table_name)
        if primary_keys:
            order_by_fields = ', '.join(primary_keys)
            query += f" ORDER BY {order_by_fields}"
            logger.info(f"添加ORDER BY主键: {order_by_fields}")
        # 如果没有主键，使用所有字段进行排序
        else:
            # 为PostgreSQL添加ORDER BY以确保结果顺序一致
            if isinstance(self.db, PostgreSQLAdapter):
                order_by_fields = ', '.join(fields)
                query += f" ORDER BY {order_by_fields}"
                logger.info(f"添加ORDER BY所有字段: {order_by_fields}")
            # 为其他数据库也添加排序以确保一致性
            else:
                # 对于非PostgreSQL数据库，如果有主键就按主键排序，否则不强制排序
                pass
        
        logger.info(f"构建完成的查询: {query}")
        return query

    def compare(self) -> Dict[str, Any]:
        """
        执行表对比
        
        :return: 对比结果
        """
        try:
            logger.info("开始执行表对比")
            # 获取两个表的所有字段
            logger.info(f"获取表 {self.table1} 的字段")
            fields1 = self.get_table_fields(self.table1)
            logger.info(f"获取表 {self.table2} 的字段")
            fields2 = self.get_table_fields(self.table2)
            
            # 检查字段是否完全一致（仅在未指定fields且未指定exclude_fields时检查）
            if not self.fields and not self.exclude_fields and set(fields1) != set(fields2):
                logger.warning(f"表 {self.table1} 和 {self.table2} 的字段不完全一致")
                result = {
                    'fields': [],
                    'table1_row_count': 0,
                    'table2_row_count': 0,
                    'differences': [{
                        'type': 'field_mismatch',
                        'message': f'表 {self.table1} 和 {self.table2} 的字段不完全一致'
                    }],
                    'row_differences': [],
                    'table1_fields': fields1,
                    'table2_fields': fields2
                }
                return result
            
            # 获取要对比的字段
            logger.info("获取对比字段")
            comparison_fields = self.get_comparison_fields()
            logger.info(f"对比字段: {comparison_fields}")
            
            # 如果指定了字段或有公共字段，但最终没有可对比字段
            if not comparison_fields:
                logger.error("没有找到可对比的字段")
                raise ValueError("没有找到可对比的字段")

            # 构建查询语句
            logger.info("构建查询语句")
            query1 = self.build_query(comparison_fields, self.table1)
            query2 = self.build_query(comparison_fields, self.table2)

            # 执行查询
            logger.info("执行查询1")
            cursor1 = self.db.execute_query(query1)
            rows1 = cursor1.fetchall()
            logger.info(f"查询1返回 {len(rows1)} 行数据")
            
            logger.info("执行查询2")
            cursor2 = self.db.execute_query(query2)
            rows2 = cursor2.fetchall()
            logger.info(f"查询2返回 {len(rows2)} 行数据")

            # 准备结果
            result = {
                'fields': comparison_fields,
                'table1_row_count': len(rows1),
                'table2_row_count': len(rows2),
                'differences': [],
                'row_differences': [],
                'table1_fields': fields1,  # 添加表1的所有字段
                'table2_fields': fields2   # 添加表2的所有字段
            }

            # 行数对比
            if len(rows1) != len(rows2):
                logger.info(f"行数不同: {self.table1}有{len(rows1)}行, {self.table2}有{len(rows2)}行")
                result['differences'].append({
                    'type': 'row_count',
                    'message': f'行数不同: {self.table1}有{len(rows1)}行, {self.table2}有{len(rows2)}行'
                })
                # 将行数据转换为字典列表以便比较
                logger.info(f"对比前 {min(len(rows1), len(rows2))} 行数据")
                dict_rows1 = [dict(zip(comparison_fields, row)) for row in rows1]
                dict_rows2 = [dict(zip(comparison_fields, row)) for row in rows2]
                
                # 对比所有可用行
                diff_count = 0
                all_row_differences = []
                
                for i in range(min(len(rows1), len(rows2))):
                    row_diff = self._compare_rows(dict_rows1[i], dict_rows2[i], i+1)
                    if row_diff:
                        diff_count += 1
                        all_row_differences.append(row_diff)
                
                # 如果第一个表行数较多，标记多余行
                for i in range(len(rows2), len(rows1)):
                    all_row_differences.append({
                        'row_number': i+1,
                        'differences': [{'field': field, 'table1_value': dict_rows1[i][field], 'table2_value': None} 
                                       for field in comparison_fields]
                    })
                    diff_count += 1
                
                # 如果第二个表行数较多，标记多余行
                for i in range(len(rows1), len(rows2)):
                    all_row_differences.append({
                        'row_number': i+1,
                        'differences': [{'field': field, 'table1_value': None, 'table2_value': dict_rows2[i][field]} 
                                       for field in comparison_fields]
                    })
                    diff_count += 1
                
                # 将所有差异添加到结果中
                result['row_differences'] = all_row_differences
                
                # 添加差异计数信息
                if diff_count > 0:
                    result['differences'].append({
                        'type': 'multiple_row_diff',
                        'count': diff_count,
                        'message': f'共有{diff_count}行存在数据差异'
                    })
            else:
                # 行数相同时，进行逐行对比，记录所有差异
                diff_count = 0
                all_row_differences = []  # 用于收集所有差异以生成CSV报告
                
                if len(rows1) > 0:
                    # 将行数据转换为字典列表以便比较
                    logger.info(f"对比 {len(rows1)} 行数据")
                    dict_rows1 = [dict(zip(comparison_fields, row)) for row in rows1]
                    dict_rows2 = [dict(zip(comparison_fields, row)) for row in rows2]
                    
                    for i, (row1, row2) in enumerate(zip(dict_rows1, dict_rows2)):
                        row_diff = self._compare_rows(row1, row2, i+1)
                        if row_diff:
                            diff_count += 1
                            all_row_differences.append(row_diff)  # 添加到所有差异列表
                
                # 将所有差异添加到结果中（用于CSV报告）
                result['row_differences'] = all_row_differences
                
                # 添加差异计数信息
                if diff_count > 0:
                    result['differences'].append({
                        'type': 'multiple_row_diff',
                        'count': diff_count,
                        'message': f'共有{diff_count}行存在数据差异'
                    })

            logger.info("表对比完成")
            return result

        except Exception as e:
            logger.error(f"对比过程中发生错误: {str(e)}", exc_info=True)
            raise RuntimeError(f"对比过程中发生错误: {str(e)}")

    def _compare_rows(self, row1: Dict, row2: Dict, row_number: int) -> Optional[Dict]:
        """
        对比两行数据
        
        :param row1: 第一行数据
        :param row2: 第二行数据
        :param row_number: 行号
        :return: 差异信息，如果没有差异则返回None
        """
        logger.debug(f"对比第 {row_number} 行数据")
        differences = []
        
        for field in row1.keys():
            value1 = row1[field]
            value2 = row2[field]
            
            if value1 != value2:
                logger.debug(f"字段 {field} 值不同: {value1} vs {value2}")
                differences.append({
                    'field': field,
                    'table1_value': value1,
                    'table2_value': value2
                })
        
        if differences:
            logger.info(f"第 {row_number} 行发现 {len(differences)} 个差异")
            return {
                'row_number': row_number,
                'differences': differences
            }
        
        logger.debug(f"第 {row_number} 行无差异")
        return None

    def generate_csv_report(self, result: Dict[str, Any], output_file: str) -> None:
        """
        生成CSV格式的详细差异报告
        
        :param result: 对比结果
        :param output_file: 输出文件路径
        """
        import csv
        
        logger.info(f"生成CSV报告到文件: {output_file}")
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['row_number', 'column_name', 'table1_value', 'table2_value']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # 遍历所有行差异
            if 'row_differences' in result:
                for row_diff in result['row_differences']:
                    row_number = row_diff['row_number']
                    for diff in row_diff['differences']:
                        writer.writerow({
                            'row_number': row_number,
                            'column_name': diff['field'],
                            'table1_value': diff['table1_value'],
                            'table2_value': diff['table2_value']
                        })
        
        logger.info("CSV报告生成完成")


def create_sample_database(db_path: str):
    """
    创建示例数据库用于测试
    
    :param db_path: 数据库文件路径
    """
    logger.info(f"创建示例数据库: {db_path}")
    conn = sqlite3.connect(db_path)
    
    # 创建示例表1
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users_old (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            created_at TEXT
        )
    ''')
    
    # 创建示例表2
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users_new (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            age INTEGER,
            phone TEXT,
            created_at TEXT
        )
    ''')
    
    # 插入示例数据到users_old
    conn.execute("INSERT INTO users_old (name, email, age, created_at) VALUES (?, ?, ?, ?)",
                 ("张三", "zhangsan@example.com", 25, "2023-01-01"))
    conn.execute("INSERT INTO users_old (name, email, age, created_at) VALUES (?, ?, ?, ?)",
                 ("李四", "lisi@example.com", 30, "2023-01-02"))
    
    # 插入示例数据到users_new
    conn.execute("INSERT INTO users_new (name, email, age, phone, created_at) VALUES (?, ?, ?, ?, ?)",
                 ("张三", "zhangsan@example.com", 25, "13800138000", "2023-01-01"))
    conn.execute("INSERT INTO users_new (name, email, age, phone, created_at) VALUES (?, ?, ?, ?, ?)",
                 ("李四", "lisi@example.com", 31, "13800138001", "2023-01-02"))  # 年龄不同
    conn.execute("INSERT INTO users_new (name, email, age, phone, created_at) VALUES (?, ?, ?, ?, ?)",
                 ("王五", "wangwu@example.com", 28, "13800138002", "2023-01-03"))  # 新增用户
    
    conn.commit()
    conn.close()
    logger.info("示例数据库创建完成")


def main():
    """
    主函数，处理命令行参数并执行对比
    """
    parser = argparse.ArgumentParser(description='数据库表对比工具')
    parser.add_argument('--db-type', choices=['sqlite', 'mysql', 'postgresql'], 
                       default='sqlite', help='数据库类型 (默认: sqlite)')
    
    # SQLite参数
    parser.add_argument('--db-path', help='SQLite数据库文件路径')
    
    # MySQL和PostgreSQL参数
    parser.add_argument('--host', help='数据库主机地址')
    parser.add_argument('--port', type=int, help='数据库端口')
    parser.add_argument('--user', help='数据库用户名')
    parser.add_argument('--password', help='数据库密码')
    parser.add_argument('--database', help='数据库名')
    
    parser.add_argument('--table1', required=True, help='第一个表名')
    parser.add_argument('--table2', required=True, help='第二个表名')
    parser.add_argument('--fields', nargs='*', help='指定要对比的字段（默认对比所有字段）')
    parser.add_argument('--exclude', nargs='*', help='指定要排除的字段')
    parser.add_argument('--where', help='WHERE条件')
    parser.add_argument('--create-sample', action='store_true', help='创建示例数据库')
    parser.add_argument('--detailed', action='store_true', help='显示详细差异信息')
    parser.add_argument('--csv-report', help='生成CSV格式的详细差异报告到指定文件')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志')

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("启用详细日志模式")
    else:
        logging.getLogger().setLevel(logging.INFO)

    # 如果需要创建示例数据库
    if args.create_sample:
        if not args.db_path:
            args.db_path = 'sample.db'
        create_sample_database(args.db_path)
        print(f"示例数据库已创建: {args.db_path}")
        return

    try:
        # 获取对应的数据库适配器
        logger.info(f"获取 {args.db_type} 数据库适配器")
        db_adapter = get_database_adapter(args.db_type)
        
        # 建立数据库连接
        if args.db_type == 'sqlite':
            if not args.db_path:
                raise ValueError("SQLite数据库需要指定 --db-path 参数")
            db_adapter.connect(db_path=args.db_path)
        else:
            if not all([args.host, args.user, args.password, args.database]):
                raise ValueError("MySQL和PostgreSQL需要指定 --host, --user, --password, --database 参数")
            connect_params = {
                'host': args.host,
                'user': args.user,
                'password': args.password,
                'database': args.database
            }
            if args.port:
                connect_params['port'] = args.port
            db_adapter.connect(**connect_params)
        
        # 创建对比器实例
        logger.info("创建表对比器实例")
        comparator = TableComparator(db_adapter)
        comparator.set_tables(args.table1, args.table2)
        
        if args.fields:
            comparator.set_fields(args.fields)
            
        if args.exclude:
            comparator.set_exclude_fields(args.exclude)
            
        if args.where:
            comparator.set_where_condition(args.where)

        # 执行对比
        print(f"开始对比表 {args.table1} 和 {args.table2}...")
        logger.info(f"开始对比表 {args.table1} 和 {args.table2}")
        result = comparator.compare()
        logger.info("对比完成")
        
        # 输出结果
        print("对比完成:")
        
        # 如果是因为字段不一致而返回的结果
        if result['differences'] and result['differences'][0]['type'] == 'field_mismatch':
            print(f"发现差异: {result['differences'][0]['message']}")
            print(f"表 {args.table1} 的字段: {', '.join(result['table1_fields'])}")
            print(f"表 {args.table2} 的字段: {', '.join(result['table2_fields'])}")
            print("请使用 --fields 参数指定要对比的字段。")
            db_adapter.close()
            return
            
        print(f"字段列表: {', '.join(result['fields'])}")
        print(f"表 {args.table1} 记录数: {result['table1_row_count']}")
        print(f"表 {args.table2} 记录数: {result['table2_row_count']}")
        
        if result['differences']:
            print("发现差异:")
            for diff in result['differences']:
                # 跳过multiple_row_diff类型的差异，因为它只用于计数
                if diff['type'] != 'multiple_row_diff':
                    print(f"- {diff['message']}")
        
        # 显示详细行差异
        if result['row_differences']:
            if args.detailed:
                print("\n详细行差异:")
                for row_diff in result['row_differences']:
                    print(f"第 {row_diff['row_number']} 行:")
                    for diff in row_diff['differences']:
                        print(f"  字段 '{diff['field']}': {args.table1}={diff['table1_value']}, {args.table2}={diff['table2_value']}")
                # 如果有多个差异，显示总数量
                multiple_diff_info = [diff for diff in result['differences'] if diff['type'] == 'multiple_row_diff']
                if multiple_diff_info:
                    print(f"\n总共 {multiple_diff_info[0]['count']} 行存在数据差异")
            else:
                # 显示第一条差异的简要信息
                first_diff = result['row_differences'][0]
                multiple_diff_info = [diff for diff in result['differences'] if diff['type'] == 'multiple_row_diff']
                if multiple_diff_info:
                    print(f"\n发现第{first_diff['row_number']}行存在数据差异，共{multiple_diff_info[0]['count']}行有差异 (使用 --detailed 参数查看详细信息)")
                else:
                    print(f"\n发现第{first_diff['row_number']}行存在数据差异 (使用 --detailed 参数查看详细信息)")
        elif not result['differences']:
            print("未发现明显差异")

        # 生成CSV报告
        if args.csv_report:
            try:
                comparator.generate_csv_report(result, args.csv_report)
                print(f"\n已生成CSV详细差异报告到: {args.csv_report}")
            except Exception as e:
                logger.error(f"生成CSV报告失败: {str(e)}", exc_info=True)
                print(f"生成CSV报告失败: {str(e)}")

        # 关闭数据库连接
        logger.info("关闭数据库连接")
        db_adapter.close()
        
    except Exception as e:
        logger.error(f"执行出错: {str(e)}", exc_info=True)
        print(f"执行出错: {str(e)}")
        exit(1)


def run_comparison(
    db_type: str,
    db_path: str = None,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    table1: str = None,
    table2: str = None,
    fields: List[str] = None,
    exclude: List[str] = None,
    where: str = None,
    csv_report: str = None
) -> Dict[str, Any]:
    """
    以编程方式运行表对比工具

    :param db_type: 数据库类型 ('sqlite', 'mysql', 'postgresql')
    :param db_path: SQLite数据库文件路径（仅用于SQLite）
    :param host: 数据库主机地址（非SQLite使用）
    :param port: 数据库端口（非SQLite使用）
    :param user: 数据库用户名（非SQLite使用）
    :param password: 数据库密码（非SQLite使用）
    :param database: 数据库名（非SQLite使用）
    :param table1: 第一个表名
    :param table2: 第二个表名
    :param fields: 指定要对比的字段列表
    :param exclude: 指定要排除的字段列表
    :param where: WHERE条件字符串
    :param csv_report: CSV报告输出文件路径
    :return: 对比结果字典
    """
    logger.info("开始以编程方式运行表对比")
    
    # 获取对应的数据库适配器
    db_adapter = get_database_adapter(db_type)
    
    # 建立数据库连接
    if db_type == 'sqlite':
        if not db_path:
            raise ValueError("SQLite数据库需要指定 db_path 参数")
        db_adapter.connect(db_path=db_path)
    else:
        if not all([host, user, password, database]):
            raise ValueError("MySQL和PostgreSQL需要指定 host, user, password, database 参数")
        connect_params = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        if port:
            connect_params['port'] = port
        db_adapter.connect(**connect_params)

    # 创建对比器实例
    comparator = TableComparator(db_adapter)
    comparator.set_tables(table1, table2)
    
    if fields:
        comparator.set_fields(fields)
        
    if exclude:
        comparator.set_exclude_fields(exclude)
        
    if where:
        comparator.set_where_condition(where)

    # 执行对比
    logger.info(f"开始对比表 {table1} 和 {table2}")
    result = comparator.compare()
    logger.info("对比完成")
    
    # 生成CSV报告
    if csv_report:
        try:
            comparator.generate_csv_report(result, csv_report)
            logger.info(f"已生成CSV详细差异报告到: {csv_report}")
        except Exception as e:
            logger.error(f"生成CSV报告失败: {str(e)}", exc_info=True)
    
    # 关闭数据库连接
    logger.info("关闭数据库连接")
    db_adapter.close()
    
    return result


if __name__ == "__main__":
    main()