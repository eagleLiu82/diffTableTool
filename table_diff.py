#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sqlite3
from typing import List, Optional, Dict, Any, Union
from abc import ABC, abstractmethod
import logging
import sys
import os

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
            database=database,
            buffered=True  # 添加buffered参数以避免游标问题
        )
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取MySQL表 {table_name} 的字段")
        cursor = self.connection.cursor(buffered=True)  # 使用buffered游标
        cursor.execute(f"DESCRIBE {table_name}")
        fields = [row[0] for row in cursor.fetchall()]
        cursor.close()  # 关闭游标
        logger.info(f"表 {table_name} 的字段: {fields}")
        return fields
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取MySQL表的主键字段"""
        logger.info(f"获取MySQL表 {table_name} 的主键")
        cursor = self.connection.cursor(buffered=True)  # 使用buffered游标
        cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
        primary_keys = [row[4] for row in cursor.fetchall()]  # Column_name列
        cursor.close()  # 关闭游标
        logger.info(f"表 {table_name} 的主键: {primary_keys}")
        return primary_keys
    
    def execute_query(self, query: str):
        logger.info(f"执行MySQL查询: {query}")
        cursor = self.connection.cursor(buffered=True)  # 使用buffered游标
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
            
            fields = [row[0] for row in cursor.fetchall()]
            if fields:
                logger.info(f"通过pg_attribute获取到表 {table_name} 的字段: {fields}")
                return fields
        except Exception as e:
            logger.warning(f"通过pg_attribute获取字段失败: {e}")
            
        # 如果都失败了，抛出表不存在的异常
        raise ValueError(f"表 '{table_name}' 不存在")
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取PostgreSQL表的主键字段"""
        logger.info(f"获取PostgreSQL表 {table_name} 的主键")
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (table_name,))
            
            primary_keys = [row[0] for row in cursor.fetchall()]
            logger.info(f"表 {table_name} 的主键: {primary_keys}")
            return primary_keys
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "does not exist" in error_msg or "not found" in error_msg or "unknown" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 主键信息时出错: {str(e)}")
    
    def execute_query(self, query: str):
        logger.info(f"执行PostgreSQL查询: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
    
    def close(self):
        if self.connection:
            logger.info("关闭PostgreSQL数据库连接")
            self.connection.close()


class OracleAdapter(DatabaseAdapter):
    """Oracle数据库适配器"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self, **kwargs):
        try:
            import oracledb
        except ImportError:
            raise ImportError("需要安装oracledb库: pip install oracledb")
        
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 1521)
        user = kwargs.get('user')
        password = kwargs.get('password')
        database = kwargs.get('database')
        service_name = kwargs.get('service_name')
        
        logger.info(f"连接到Oracle数据库: {host}:{port}, 用户: {user}, 数据库: {database}")
        
        # 构建DSN
        if service_name:
            dsn = oracledb.makedsn(host, port, service_name=service_name)
        else:
            dsn = oracledb.makedsn(host, port, sid=database)
            
        self.connection = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn
        )
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取Oracle表 {table_name} 的字段")
        try:
            cursor = self.connection.cursor()
            # 处理可能包含模式的表名
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    owner, table = parts
                else:
                    owner, table = None, table_name
            else:
                owner, table = None, table_name
                
            if owner:
                cursor.execute("""
                    SELECT column_name 
                    FROM all_tab_columns 
                    WHERE table_name = UPPER(%s) AND owner = UPPER(%s)
                    ORDER BY column_id
                """, (table, owner))
            else:
                cursor.execute("""
                    SELECT column_name 
                    FROM user_tab_columns 
                    WHERE table_name = UPPER(%s)
                    ORDER BY column_id
                """, (table,))
                
            fields = [row[0] for row in cursor.fetchall()]
            logger.info(f"表 {table_name} 的字段: {fields}")
            return fields
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "not exist" in error_msg or "not found" in error_msg or "invalid" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 字段信息时出错: {str(e)}")
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取Oracle表的主键字段"""
        logger.info(f"获取Oracle表 {table_name} 的主键")
        try:
            cursor = self.connection.cursor()
            # 处理可能包含模式的表名
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    owner, table = parts
                else:
                    owner, table = None, table_name
            else:
                owner, table = None, table_name
                
            if owner:
                cursor.execute("""
                    SELECT cols.column_name
                    FROM all_constraints cons
                    JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
                    WHERE cols.table_name = UPPER(%s) AND cols.owner = UPPER(%s) AND cons.constraint_type = 'P'
                    ORDER BY cols.position
                """, (table, owner))
            else:
                cursor.execute("""
                    SELECT column_name
                    FROM user_cons_columns
                    WHERE table_name = UPPER(%s) AND constraint_name IN (
                        SELECT constraint_name 
                        FROM user_constraints 
                        WHERE constraint_type = 'P'
                    )
                    ORDER BY position
                """, (table,))
                
            primary_keys = [row[0] for row in cursor.fetchall()]
            logger.info(f"表 {table_name} 的主键: {primary_keys}")
            return primary_keys
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "not exist" in error_msg or "not found" in error_msg or "invalid" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 主键信息时出错: {str(e)}")
    
    def execute_query(self, query: str):
        logger.info(f"执行Oracle查询: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
    
    def close(self):
        if self.connection:
            logger.info("关闭Oracle数据库连接")
            self.connection.close()


class MSSQLAdapter(DatabaseAdapter):
    """MSSQL数据库适配器"""
    
    def __init__(self):
        self.connection = None
    
    def connect(self, **kwargs):
        try:
            import pymssql
        except ImportError:
            raise ImportError("需要安装pymssql库: pip install pymssql")
        
        host = kwargs.get('host', 'localhost')
        port = kwargs.get('port', 1433)
        user = kwargs.get('user')
        password = kwargs.get('password')
        database = kwargs.get('database')
        
        # 构建服务器地址
        if port != 1433:
            server = f"{host}:{port}"
        else:
            server = host
            
        logger.info(f"连接到MSSQL数据库: {server}, 用户: {user}, 数据库: {database}")
        
        self.connection = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database
        )
        return self.connection
    
    def get_table_fields(self, table_name: str) -> List[str]:
        logger.info(f"获取MSSQL表 {table_name} 的字段")
        try:
            cursor = self.connection.cursor()
            
            # 处理可能包含模式的表名
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    schema, table = parts
                else:
                    schema, table = 'dbo', table_name
            else:
                schema, table = 'dbo', table_name
                
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
                ORDER BY ORDINAL_POSITION
            """, (table, schema))
            
            fields = [row[0] for row in cursor.fetchall()]
            logger.info(f"表 {table_name} 的字段: {fields}")
            return fields
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "not exist" in error_msg or "not found" in error_msg or "invalid" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 字段信息时出错: {str(e)}")
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """获取MSSQL表的主键字段"""
        logger.info(f"获取MSSQL表 {table_name} 的主键")
        try:
            cursor = self.connection.cursor()
            
            # 处理可能包含模式的表名
            if '.' in table_name:
                parts = table_name.split('.')
                if len(parts) == 2:
                    schema, table = parts
                else:
                    schema, table = 'dbo', table_name
            else:
                schema, table = 'dbo', table_name
                
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                AND TABLE_NAME = %s AND TABLE_SCHEMA = %s
                ORDER BY ORDINAL_POSITION
            """, (table, schema))
            
            primary_keys = [row[0] for row in cursor.fetchall()]
            logger.info(f"表 {table_name} 的主键: {primary_keys}")
            return primary_keys
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "not exist" in error_msg or "not found" in error_msg or "invalid" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 主键信息时出错: {str(e)}")
    
    def execute_query(self, query: str):
        logger.info(f"执行MSSQL查询: {query}")
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor
    
    def close(self):
        if self.connection:
            logger.info("关闭MSSQL数据库连接")
            self.connection.close()


def get_database_adapter(db_type: str) -> DatabaseAdapter:
    """根据数据库类型获取对应的适配器"""
    logger.info(f"获取数据库适配器: {db_type}")
    adapters = {
        'sqlite': SQLiteAdapter,
        'mysql': MySQLAdapter,
        'postgresql': PostgreSQLAdapter,
        'oracle': OracleAdapter,
        'mssql': MSSQLAdapter
    }
    
    if db_type not in adapters:
        raise ValueError(f"不支持的数据库类型: {db_type}")
    
    return adapters[db_type]()


# 自定义Action类用于处理逗号分隔的参数
class CommaSeparatedArgsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if isinstance(values, str):
            # 如果输入是字符串，则按逗号分割
            values = [v.strip() for v in values.split(',') if v.strip()]
        setattr(namespace, self.dest, values)


class TableComparator:
    """
    数据库表对比工具类
    支持对比同一数据库中的两个表，可以指定字段、排除字段和设置WHERE条件
    """

    def __init__(self, db_adapter: DatabaseAdapter, db_adapter2: DatabaseAdapter = None):
        """
        初始化对比工具
        
        :param db_adapter: 源数据库适配器实例
        :param db_adapter2: 目标数据库适配器实例（可选，默认为None表示使用同一个数据库）
        """
        self.db1 = db_adapter
        self.db2 = db_adapter2 if db_adapter2 is not None else db_adapter
        self.table1 = None
        self.table2 = None
        self.fields = []
        self.exclude_fields = []
        self.where_condition = None
        # 支持两个表的不同WHERE条件
        self.where_condition1 = None
        self.where_condition2 = None
        logger.info("TableComparator初始化完成")

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
        设置WHERE条件（两个表使用相同的WHERE条件）
        
        :param where_condition: WHERE条件字符串
        """
        logger.info(f"设置WHERE条件: {where_condition}")
        self.where_condition = where_condition

    def set_where_condition1(self, where_condition: str):
        """
        设置第一个表的WHERE条件
        
        :param where_condition: WHERE条件字符串
        """
        logger.info(f"设置表 {self.table1} 的WHERE条件: {where_condition}")
        self.where_condition1 = where_condition

    def set_where_condition2(self, where_condition: str):
        """
        设置第二个表的WHERE条件
        
        :param where_condition: WHERE条件字符串
        """
        logger.info(f"设置表 {self.table2} 的WHERE条件: {where_condition}")
        self.where_condition2 = where_condition

    def get_table_fields(self, table_name: str, db_index: int = 1) -> List[str]:
        """
        获取表的所有字段名
        
        :param table_name: 表名
        :param db_index: 数据库索引 (1表示源数据库, 2表示目标数据库)
        :return: 字段名列表
        """
        logger.info(f"获取表 {table_name} 的所有字段")
        try:
            if db_index == 1:
                fields = self.db1.get_table_fields(table_name)
            else:
                fields = self.db2.get_table_fields(table_name)
            logger.info(f"表 {table_name} 的字段: {fields}")
            return fields
        except Exception as e:
            # 检查是否是表不存在的错误
            error_msg = str(e).lower()
            if "no such table" in error_msg or "doesn't exist" in error_msg or "not found" in error_msg or "unknown" in error_msg or "表" in error_msg:
                raise ValueError(f"表 '{table_name}' 不存在")
            else:
                raise RuntimeError(f"获取表 '{table_name}' 字段信息时出错: {str(e)}")

    def get_comparison_fields(self) -> List[str]:
        """
        获取最终要对比的字段列表
        
        :return: 对比字段列表
        """
        logger.info("获取对比字段列表")
        # 如果用户指定了字段，则直接使用
        if self.fields:
            logger.info(f"使用指定的字段: {self.fields}")
            # 检查指定的字段是否在两个表中都存在
            try:
                fields1 = self.get_table_fields(self.table1, 1)
                fields2 = self.get_table_fields(self.table2, 2)
            except Exception as e:
                logger.error(f"获取表字段时出错: {str(e)}")
                raise
            
            # 检查每个指定的字段是否存在于两个表中
            missing_in_table1 = [f for f in self.fields if f not in fields1]
            missing_in_table2 = [f for f in self.fields if f not in fields2]
            
            if missing_in_table1:
                raise ValueError(f"字段 {', '.join(missing_in_table1)} 在表 '{self.table1}' 中不存在")
                
            if missing_in_table2:
                raise ValueError(f"字段 {', '.join(missing_in_table2)} 在表 '{self.table2}' 中不存在")
                
            comparison_fields = list(self.fields)
        else:
            # 否则获取两个表的公共字段
            logger.info(f"获取表 {self.table1} 的字段")
            fields1 = self.get_table_fields(self.table1, 1)
            logger.info(f"获取表 {self.table2} 的字段")
            fields2 = self.get_table_fields(self.table2, 2)

            # 获取公共字段
            common_fields = list(set(fields1) & set(fields2))
            logger.info(f"两个表的公共字段: {common_fields}")

            # 如果有排除字段，则移除它们
            if self.exclude_fields:
                logger.info(f"排除字段: {self.exclude_fields}")
                # 检查要排除的字段是否存在于公共字段中
                existing_exclude_fields = [f for f in self.exclude_fields if f in common_fields]
                non_existing_exclude_fields = [f for f in self.exclude_fields if f not in common_fields]
                
                if non_existing_exclude_fields:
                    logger.warning(f"要排除的字段 {', '.join(non_existing_exclude_fields)} 不存在于两个表的公共字段中")
                
                common_fields = [f for f in common_fields if f not in existing_exclude_fields]
                logger.info(f"排除后剩余字段: {common_fields}")

            comparison_fields = common_fields

        logger.info(f"最终对比字段: {comparison_fields}")
        
        # 如果表有主键但主键不在比较字段中，则添加主键字段
        # 仅当用户没有指定字段时才添加主键，如果用户指定了字段，则完全按照用户指定的字段进行比较
        if not self.fields:
            primary_keys = self.db1.get_primary_keys(self.table1)
            if primary_keys:
                for pk in primary_keys:
                    if pk not in comparison_fields:
                        logger.info(f"添加主键字段 {pk} 到比较字段中")
                        comparison_fields.append(pk)
        
        return comparison_fields

    def build_query(self, fields: List[str], table_name: str, db_index: int = 1) -> str:
        """
        构建查询SQL
        
        :param fields: 字段列表
        :param table_name: 表名
        :param db_index: 数据库索引 (1表示源数据库, 2表示目标数据库)
        :return: 查询SQL语句
        """
        logger.info(f"为表 {table_name} 构建查询，字段: {fields}")
        
        # 获取主键字段
        primary_keys = self.db1.get_primary_keys(table_name) if db_index == 1 else self.db2.get_primary_keys(table_name)
        
        # 确保主键字段包含在查询字段中，以避免KeyError
        query_fields = list(fields)
        for pk in primary_keys:
            if pk not in query_fields:
                query_fields.append(pk)
        
        field_list = ', '.join(query_fields)
        query = f"SELECT {field_list} FROM {table_name}"
        
        # 添加WHERE条件，优先使用特定表的WHERE条件
        where_condition = None
        logger.info(f"表名匹配详情 - 当前表名: '{table_name}', self.table1: '{self.table1}', self.table2: '{self.table2}'")
        logger.info(f"WHERE条件值 - where_condition1: {self.where_condition1}, where_condition2: {self.where_condition2}, where_condition: {self.where_condition}")
        
        # 改进表名匹配逻辑，处理可能的模式名前缀
        # 确保表名不为空
        if self.table1 and self.table2:
            table1_name = self.table1.split('.')[-1]  # 获取表名部分（去掉模式前缀）
            table2_name = self.table2.split('.')[-1]  # 获取表名部分（去掉模式前缀）
            current_table_name = table_name.split('.')[-1]  # 获取当前表名部分（去掉模式前缀）
            
            logger.info(f"简化表名匹配 - 当前表名: '{current_table_name}', 表1名: '{table1_name}', 表2名: '{table2_name}'")
            
            if current_table_name == table1_name:
                where_condition = self.where_condition1 or self.where_condition
                logger.info(f"匹配到表1，使用表1的WHERE条件: {where_condition}")
            elif current_table_name == table2_name:
                where_condition = self.where_condition2 or self.where_condition
                logger.info(f"匹配到表2，使用表2的WHERE条件: {where_condition}")
            else:
                where_condition = self.where_condition
                logger.info(f"未匹配到特定表，使用通用WHERE条件: {where_condition}")
        else:
            where_condition = self.where_condition
            logger.info(f"表名未设置，使用通用WHERE条件: {where_condition}")
            
        if where_condition:
            query += f" WHERE {where_condition}"
            logger.info(f"最终添加的WHERE条件: {where_condition}")
        
        # 添加ORDER BY主键
        if primary_keys:
            order_by_fields = ', '.join(primary_keys)
            query += f" ORDER BY {order_by_fields}"
            logger.info(f"添加ORDER BY主键: {order_by_fields}")
        # 如果没有主键，使用所有字段进行排序
        else:
            # 为PostgreSQL添加ORDER BY以确保结果顺序一致
            db = self.db1 if db_index == 1 else self.db2
            if isinstance(db, PostgreSQLAdapter):
                order_by_fields = ', '.join(query_fields)
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
            fields1 = self.get_table_fields(self.table1, 1)
            logger.info(f"获取表 {self.table2} 的字段")
            fields2 = self.get_table_fields(self.table2, 2)
            
            # 只有在用户没有指定字段且没有指定排除字段时，才检查字段一致性
            if not self.fields and not self.exclude_fields:
                # 检查字段是否完全一致
                if set(fields1) != set(fields2):
                    # 获取公共字段
                    common_fields = list(set(fields1) & set(fields2))
                    
                    # 字段不一致，返回字段不匹配
                    logger.warning(f"表 {self.table1} 和 {self.table2} 字段不一致")
                    # 获取差异字段
                    only_in_table1 = list(set(fields1) - set(fields2))
                    only_in_table2 = list(set(fields2) - set(fields1))
                    
                    result = {
                        'fields': [],
                        'table1_row_count': 0,
                        'table2_row_count': 0,
                        'differences': [{
                            'type': 'field_mismatch',
                            'message': f'表 {self.table1} 和 {self.table2} 字段不一致',
                            'details': {
                                'table1_fields': fields1,
                                'table2_fields': fields2,
                                'only_in_table1': only_in_table1,
                                'only_in_table2': only_in_table2,
                                'common_fields': common_fields
                            }
                        }],
                        'row_differences': [],
                        'table1_fields': fields1,
                        'table2_fields': fields2,
                        'only_in_table1': only_in_table1,
                        'only_in_table2': only_in_table2,
                        'common_fields': common_fields
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
            query1 = self.build_query(comparison_fields, self.table1, 1)
            query2 = self.build_query(comparison_fields, self.table2, 2)
            
            # 执行查询获取游标，但不立即获取所有数据
            logger.info("执行查询1")
            cursor1 = self.db1.execute_query(query1)
            logger.info("执行查询2")
            cursor2 = self.db2.execute_query(query2)
            
            # 获取主键字段
            primary_keys1 = self.db1.get_primary_keys(self.table1)
            primary_keys2 = self.db2.get_primary_keys(self.table2)
            common_primary_keys = list(set(primary_keys1) & set(primary_keys2))
            
            # 准备结果
            result = {
                'fields': comparison_fields,
                'table1_row_count': 0,
                'table2_row_count': 0,
                'differences': [],
                'row_differences': [],
                'table1_fields': fields1,  # 添加表1的所有字段
                'table2_fields': fields2   # 添加表2的所有字段
            }
            
            # 如果两个表都有主键，且主键字段一致，并且主键字段在比较字段中，则按主键进行匹配对比
            if common_primary_keys and all(pk in comparison_fields for pk in common_primary_keys):
                logger.info(f"使用主键 {common_primary_keys} 进行匹配对比")
                comparison_result = self._compare_rows_by_primary_key_streaming(
                    cursor1, cursor2, common_primary_keys, comparison_fields)
                result['row_differences'] = comparison_result['differences']
                result['table1_row_count'] = comparison_result['table1_row_count']
                result['table2_row_count'] = comparison_result['table2_row_count']
            else:
                # 否则按行位置进行对比
                logger.info("没有共同主键或主键不在比较字段中，按行位置进行对比")
                comparison_result = self._compare_rows_by_position_streaming(
                    cursor1, cursor2, comparison_fields)
                result['row_differences'] = comparison_result['differences']
                result['table1_row_count'] = comparison_result['table1_row_count']
                result['table2_row_count'] = comparison_result['table2_row_count']
            
            # 添加差异计数信息
            diff_count = len(result['row_differences'])
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
            
    def _compare_rows_by_primary_key_streaming(self, cursor1, cursor2, 
                                     primary_keys: List[str], comparison_fields: List[str]) -> dict:
        """
        基于主键流式对比两组行数据
        
        :param cursor1: 第一个表的游标
        :param cursor2: 第二个表的游标
        :param primary_keys: 主键字段列表
        :param comparison_fields: 需要对比的字段列表
        :return: 包含差异列表和行数统计的字典
        """
        logger.info("基于主键进行流式行数据对比")
        
        # 获取所有数据并按主键排序
        rows1_data = {}
        rows2_data = {}
        
        # 读取第一个表的数据
        row_count1 = 0
        for row in cursor1:
            row_dict = dict(zip(comparison_fields, row))
            key = tuple(row_dict[pk] for pk in primary_keys)
            rows1_data[key] = row_dict
            row_count1 += 1
            
        # 读取第二个表的数据
        row_count2 = 0
        for row in cursor2:
            row_dict = dict(zip(comparison_fields, row))
            key = tuple(row_dict[pk] for pk in primary_keys)
            rows2_data[key] = row_dict
            row_count2 += 1
            
        # 收集所有主键值
        all_keys = set(rows1_data.keys()) | set(rows2_data.keys())
        logger.info(f"总共 {len(all_keys)} 个唯一主键值")
        
        # 对比每一行
        differences = []
        row_number = 1
        
        # 分别收集三种类型的差异
        diff_rows = []  # 两个表中都存在但数据不同的记录
        only_in_table1 = []  # 源表中有但目标表中没有的记录
        only_in_table2 = []  # 目标表中有但源表中没有的记录
        
        for key in sorted(all_keys):  # 按主键排序，保证输出顺序一致
            row1 = rows1_data.get(key)
            row2 = rows2_data.get(key)
            
            if row1 is None:
                # 只在表2中存在
                diff = {
                    'row_number': row_number,
                    'type': 'only_in_table2',
                    'key': dict(zip(primary_keys, key)),
                    'differences': [{'field': field, 'table1_value': None, 'table2_value': row2[field]} 
                                   for field in comparison_fields]
                }
                only_in_table2.append(diff)
                differences.append(diff)
            elif row2 is None:
                # 只在表1中存在
                diff = {
                    'row_number': row_number,
                    'type': 'only_in_table1',
                    'key': dict(zip(primary_keys, key)),
                    'differences': [{'field': field, 'table1_value': row1[field], 'table2_value': None} 
                                   for field in comparison_fields]
                }
                only_in_table1.append(diff)
                differences.append(diff)
            else:
                # 两个表中都存在，对比字段值
                row_diff = self._compare_single_row(row1, row2, row_number, comparison_fields)
                if row_diff:
                    row_diff['type'] = 'different_data'
                    row_diff['key'] = dict(zip(primary_keys, key))
                    diff_rows.append(row_diff)
                    differences.append(row_diff)
                    
            row_number += 1
            
        logger.info(f"基于主键对比完成，发现数据不同的记录 {len(diff_rows)} 条，源表独有记录 {len(only_in_table1)} 条，目标表独有记录 {len(only_in_table2)} 条")
        return {
            'differences': differences,
            'table1_row_count': row_count1,
            'table2_row_count': row_count2
        }

    def _compare_rows_by_position_streaming(self, cursor1, cursor2, 
                                  comparison_fields: List[str]) -> dict:
        """
        基于行位置流式对比两组行数据
        
        :param cursor1: 第一个表的游标
        :param cursor2: 第二个表的游标
        :param comparison_fields: 需要对比的字段列表
        :return: 包含差异列表和行数统计的字典
        """
        logger.info("基于行位置进行流式行数据对比")
        differences = []
        
        rows1_list = list(cursor1)
        rows2_list = list(cursor2)
        
        row_count1 = len(rows1_list)
        row_count2 = len(rows2_list)
        
        # 处理共同行数范围内的对比
        for i in range(min(row_count1, row_count2)):
            row1_dict = dict(zip(comparison_fields, rows1_list[i]))
            row2_dict = dict(zip(comparison_fields, rows2_list[i]))
            row_diff = self._compare_single_row(row1_dict, row2_dict, i+1, comparison_fields)
            if row_diff:
                row_diff['type'] = 'different_data'
                differences.append(row_diff)
        
        # 处理多余的行（如果有的话）
        if row_count1 != row_count2:
            logger.info(f"行数不同: 表1有{row_count1}行, 表2有{row_count2}行")
            
            # 标记多余行
            if row_count1 > row_count2:
                for i in range(row_count2, row_count1):
                    row_dict = dict(zip(comparison_fields, rows1_list[i]))
                    differences.append({
                        'row_number': i+1,
                        'type': 'only_in_table1',
                        'differences': [{'field': field, 'table1_value': row_dict[field], 'table2_value': None} 
                                       for field in comparison_fields]
                    })
            else:
                for i in range(row_count1, row_count2):
                    row_dict = dict(zip(comparison_fields, rows2_list[i]))
                    differences.append({
                        'row_number': i+1,
                        'type': 'only_in_table2',
                        'differences': [{'field': field, 'table1_value': None, 'table2_value': row_dict[field]} 
                                       for field in comparison_fields]
                    })
        
        logger.info(f"基于行位置对比完成，发现 {len(differences)} 个差异")
        return {
            'differences': differences,
            'table1_row_count': row_count1,
            'table2_row_count': row_count2
        }

    def _compare_single_row(self, row1: Dict, row2: Dict, row_number: int, 
                            comparison_fields: List[str]) -> Optional[Dict]:
        """
        对比单行数据
        
        :param row1: 第一行数据
        :param row2: 第二行数据
        :param row_number: 行号
        :param comparison_fields: 需要对比的字段列表
        :return: 差异信息，如果没有差异则返回None
        """
        logger.debug(f"对比第 {row_number} 行数据")
        differences = []
        
        for field in comparison_fields:
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
            fieldnames = ['row_type', 'key_info', 'row_number', 'column_name', 'table1_value', 'table2_value']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            # 遍历所有行差异
            if 'row_differences' in result:
                for row_diff in result['row_differences']:
                    row_type = row_diff.get('type', 'unknown')
                    row_number = row_diff['row_number']
                    key_info = ''
                    
                    # 如果有主键信息，则记录主键信息
                    if 'key' in row_diff:
                        key_info = ', '.join([f"{k}={v}" for k, v in row_diff['key'].items()])
                    
                    for diff in row_diff['differences']:
                        csv_row = {
                            'row_type': row_type,
                            'key_info': key_info,
                            'row_number': row_number,
                            'column_name': diff['field'],
                            'table1_value': diff['table1_value'],
                            'table2_value': diff['table2_value']
                        }
                        writer.writerow(csv_row)
        
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
    # 检查是否是要启动GUI
    if len(sys.argv) > 1 and sys.argv[1] == '--gui':
        # 尝试启动GUI版本
        try:
            # 添加当前目录到Python路径
            current_dir = os.path.dirname(os.path.abspath(__file__))
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            
            # 导入并运行GUI版本
            from table_diff_gui import main as gui_main
            gui_main()
            return
        except ImportError as e:
            print(f"错误: 无法找到GUI模块，请确保 table_diff_gui.py 文件存在: {e}")
            sys.exit(1)
    
    parser = argparse.ArgumentParser(description='数据库表对比工具')
    # 源数据库参数
    parser.add_argument('--source-db-type', choices=['sqlite', 'mysql', 'postgresql', 'oracle', 'mssql'], 
                       default='sqlite', help='源数据库类型 (默认: sqlite)')
    parser.add_argument('--source-db-path', help='源SQLite数据库文件路径')
    parser.add_argument('--source-host', help='源数据库主机地址')
    parser.add_argument('--source-port', type=int, help='源数据库端口')
    parser.add_argument('--source-user', help='源数据库用户名')
    parser.add_argument('--source-password', help='源数据库密码')
    parser.add_argument('--source-database', help='源数据库名')
    parser.add_argument('--source-service-name', help='Oracle源数据库服务名')
    
    # 目标数据库参数
    parser.add_argument('--target-db-type', choices=['sqlite', 'mysql', 'postgresql', 'oracle', 'mssql'], 
                       help='目标数据库类型 (默认: 与源数据库相同)')
    parser.add_argument('--target-db-path', help='目标SQLite数据库文件路径')
    parser.add_argument('--target-host', help='目标数据库主机地址')
    parser.add_argument('--target-port', type=int, help='目标数据库端口')
    parser.add_argument('--target-user', help='目标数据库用户名')
    parser.add_argument('--target-password', help='目标数据库密码')
    parser.add_argument('--target-database', help='目标数据库名')
    parser.add_argument('--target-service-name', help='Oracle目标数据库服务名')
    
    parser.add_argument('--table1', required=True, help='第一个表名')
    parser.add_argument('--table2', required=True, help='第二个表名')
    parser.add_argument('--fields', action=CommaSeparatedArgsAction, help='指定要对比的字段，多个字段用逗号分隔（默认对比所有字段）')
    parser.add_argument('--exclude', action=CommaSeparatedArgsAction, help='指定要排除的字段，多个字段用逗号分隔')
    parser.add_argument('--where', help='两个表通用的WHERE条件')
    parser.add_argument('--where1', help='第一个表的WHERE条件')
    parser.add_argument('--where2', help='第二个表的WHERE条件')
    parser.add_argument('--create-sample', action='store_true', help='创建示例数据库')
    parser.add_argument('--detailed', action='store_true', help='显示详细差异信息')
    parser.add_argument('--csv-report', help='生成CSV格式的详细差异报告到指定文件')
    parser.add_argument('--verbose', '-v', action='store_true', help='显示详细日志')
    parser.add_argument('--gui', action='store_true', help='启动图形界面')

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("启用详细日志模式")
    else:
        logging.getLogger().setLevel(logging.INFO)

    # 如果需要创建示例数据库
    if args.create_sample:
        if not args.source_db_path:
            args.source_db_path = 'sample.db'
        create_sample_database(args.source_db_path)
        print(f"示例数据库已创建: {args.source_db_path}")
        return
    # (保持原有逻辑不变)
    try:
        # 获取对应的数据库适配器
        logger.info(f"获取 {args.source_db_type} 源数据库适配器")
        source_db_adapter = get_database_adapter(args.source_db_type)
        
        # 获取目标数据库类型（如果未指定，则与源数据库相同）
        target_db_type = args.target_db_type if args.target_db_type else args.source_db_type
        logger.info(f"获取 {target_db_type} 目标数据库适配器")
        target_db_adapter = get_database_adapter(target_db_type)
        
        # 建立源数据库连接
        if args.source_db_type == 'sqlite':
            if not args.source_db_path:
                raise ValueError("SQLite数据库需要指定 --source-db-path 参数")
            source_db_adapter.connect(db_path=args.source_db_path)
        else:
            if not all([args.source_host, args.source_user, args.source_password, args.source_database]):
                raise ValueError("MySQL和PostgreSQL需要指定 --source-host, --source-user, --source-password, --source-database 参数")
            connect_params = {
                'host': args.source_host,
                'user': args.source_user,
                'password': args.source_password,
                'database': args.source_database
            }
            if args.source_port:
                connect_params['port'] = args.source_port
            if args.source_db_type == 'oracle' and args.source_service_name:
                connect_params['service_name'] = args.source_service_name
            source_db_adapter.connect(**connect_params)
        
        # 建立目标数据库连接
        if target_db_type == 'sqlite':
            if not args.target_db_path:
                # 如果未指定目标数据库路径，则使用源数据库路径
                if args.source_db_type == 'sqlite' and args.source_db_path:
                    connect_params = {'db_path': args.source_db_path}
                else:
                    raise ValueError("目标SQLite数据库需要指定 --target-db-path 参数")
            else:
                connect_params = {'db_path': args.target_db_path}
            target_db_adapter.connect(**connect_params)
        else:
            # 对于MySQL和PostgreSQL，如果未提供目标数据库参数，则使用源数据库参数
            if not all([args.target_host, args.target_user, args.target_password, args.target_database]):
                # 检查是否源数据库也是相同类型且提供了参数
                if (args.source_db_type == target_db_type and 
                    all([args.source_host, args.source_user, args.source_password, args.source_database])):
                    # 使用源数据库的连接参数
                    connect_params = {
                        'host': args.source_host,
                        'user': args.source_user,
                        'password': args.source_password,
                        'database': args.source_database
                    }
                    if args.source_port:
                        connect_params['port'] = args.source_port
                    if target_db_type == 'oracle' and args.source_service_name:
                        connect_params['service_name'] = args.source_service_name
                else:
                    raise ValueError("目标MySQL和PostgreSQL需要指定 --target-host, --target-user, --target-password, --target-database 参数")
            else:
                connect_params = {
                    'host': args.target_host,
                    'user': args.target_user,
                    'password': args.target_password,
                    'database': args.target_database
                }
                if args.target_port:
                    connect_params['port'] = args.target_port
                if target_db_type == 'oracle' and args.target_service_name:
                    connect_params['service_name'] = args.target_service_name
            target_db_adapter.connect(**connect_params)
        
        # 创建对比器实例
        comparator = TableComparator(source_db_adapter, target_db_adapter)
        comparator.set_tables(args.table1, args.table2)
        
        if args.fields:
            comparator.set_fields(args.fields)
            
        if args.exclude:
            comparator.set_exclude_fields(args.exclude)
            
        # 设置WHERE条件
        if args.where:
            comparator.set_where_condition(args.where)
        if args.where1:
            comparator.set_where_condition1(args.where1)
        if args.where2:
            comparator.set_where_condition2(args.where2)

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
            
            # 显示差异字段
            if result['only_in_table1']:
                print(f"仅在表 {args.table1} 中存在的字段: {','.join(result['only_in_table1'])}")
            if result['only_in_table2']:
                print(f"仅在表 {args.table2} 中存在的字段: {','.join(result['only_in_table2'])}")
            if result['common_fields']:
                print(f"两个表共有的字段: {','.join(result['common_fields'])}")
                
            print("请使用 --fields 参数指定要对比的字段。")
            source_db_adapter.close()
            target_db_adapter.close()
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
                # 分类显示差异
                diff_rows = [r for r in result['row_differences'] if r.get('type') == 'different_data']
                only_in_table1 = [r for r in result['row_differences'] if r.get('type') == 'only_in_table1']
                only_in_table2 = [r for r in result['row_differences'] if r.get('type') == 'only_in_table2']
                
                if diff_rows:
                    print(f"\n[数据不同] 两个表中都存在但数据不同的记录 ({len(diff_rows)} 条):")
                    for row_diff in diff_rows:
                        if 'key' in row_diff:
                            key_str = ', '.join([f"{k}={v}" for k, v in row_diff['key'].items()])
                            print(f"  主键 {key_str}:")
                        else:
                            print(f"  第 {row_diff['row_number']} 行:")
                        for diff in row_diff['differences']:
                            print(f"    字段 '{diff['field']}': {args.table1}={diff['table1_value']}, {args.table2}={diff['table2_value']}")
                    
                if only_in_table1:
                    print(f"\n[源表独有] 在 {args.table1} 中存在但 {args.table2} 中不存在的记录 ({len(only_in_table1)} 条):")
                    for row_diff in only_in_table1:
                        if 'key' in row_diff:
                            key_str = ', '.join([f"{k}={v}" for k, v in row_diff['key'].items()])
                            print(f"  主键 {key_str}:")
                            for diff in row_diff['differences']:
                                print(f"    {diff['field']}: {diff['table1_value']}")
                        else:
                            print(f"  第 {row_diff['row_number']} 行:")
                            for diff in row_diff['differences']:
                                print(f"    {diff['field']}: {diff['table1_value']}")
                    
                if only_in_table2:
                    print(f"\n[目标表独有] 在 {args.table2} 中存在但 {args.table1} 中不存在的记录 ({len(only_in_table2)} 条):")
                    for row_diff in only_in_table2:
                        if 'key' in row_diff:
                            key_str = ', '.join([f"{k}={v}" for k, v in row_diff['key'].items()])
                            print(f"  主键 {key_str}:")
                            for diff in row_diff['differences']:
                                print(f"    {diff['field']}: {diff['table2_value']}")
                        else:
                            print(f"  第 {row_diff['row_number']} 行:")
                            for diff in row_diff['differences']:
                                print(f"    {diff['field']}: {diff['table2_value']}")
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
        source_db_adapter.close()
        target_db_adapter.close()
        
    except Exception as e:
        logger.error(f"执行出错: {str(e)}", exc_info=True)
        # 当表不存在时，确保错误信息在控制台输出
        error_msg = str(e)
        if "表" in error_msg and "不存在" in error_msg:
            print(f"错误: {error_msg}")
        elif "no such table" in error_msg or "doesn't exist" in error_msg or "not found" in error_msg:
            # 处理英文错误信息
            print(f"错误: {error_msg}")
        else:
            print(f"执行出错: {str(e)}")
        exit(1)


def run_comparison(
    source_db_type: str,
    source_db_path: str = None,
    source_host: str = None,
    source_port: Union[str, int] = None,
    source_user: str = None,
    source_password: str = None,
    source_database: str = None,
    target_db_type: str = None,
    target_db_path: str = None,
    target_host: str = None,
    target_port: Union[str, int] = None,
    target_user: str = None,
    target_password: str = None,
    target_database: str = None,
    source_service_name: str = None,
    target_service_name: str = None,
    table1: str = None,
    table2: str = None,
    fields: List[str] = None,
    exclude: List[str] = None,
    where: str = None,
    where1: str = None,
    where2: str = None,
    csv_report: str = None
) -> Dict[str, Any]:
    """
    以编程方式运行表对比工具

    :param source_db_type: 源数据库类型 ('sqlite', 'mysql', 'postgresql', 'oracle', 'mssql')
    :param source_db_path: 源SQLite数据库文件路径（仅用于SQLite）
    :param source_host: 源数据库主机地址（非SQLite使用）
    :param source_port: 源数据库端口（非SQLite使用）
    :param source_user: 源数据库用户名（非SQLite使用）
    :param source_password: 源数据库密码（非SQLite使用）
    :param source_database: 源数据库名（非SQLite使用）
    :param target_db_type: 目标数据库类型 ('sqlite', 'mysql', 'postgresql', 'oracle', 'mssql')，默认与源数据库相同
    :param target_db_path: 目标SQLite数据库文件路径（仅用于SQLite）
    :param target_host: 目标数据库主机地址（非SQLite使用）
    :param target_port: 目标数据库端口（非SQLite使用）
    :param target_user: 目标数据库用户名（非SQLite使用）
    :param target_password: 目标数据库密码（非SQLite使用）
    :param target_database: 目标数据库名（非SQLite使用）
    :param source_service_name: Oracle源数据库服务名（仅用于Oracle）
    :param target_service_name: Oracle目标数据库服务名（仅用于Oracle）
    :param table1: 第一个表名
    :param table2: 第二个表名
    :param fields: 指定要对比的字段列表
    :param exclude: 指定要排除的字段列表
    :param where: WHERE条件字符串（两个表通用）
    :param where1: 第一个表的WHERE条件字符串
    :param where2: 第二个表的WHERE条件字符串
    :param csv_report: CSV报告输出文件路径
    :return: 对比结果字典
    """
    logger.info("开始以编程方式运行表对比")
    
    # 获取源数据库适配器
    source_db_adapter = get_database_adapter(source_db_type)
    
    # 获取目标数据库类型（如果未指定，则与源数据库相同）
    if target_db_type is None:
        target_db_type = source_db_type
    
    # 获取目标数据库适配器
    target_db_adapter = get_database_adapter(target_db_type)
    
    # 建立源数据库连接
    if source_db_type == 'sqlite':
        if not source_db_path:
            raise ValueError("SQLite数据库需要指定 source_db_path 参数")
        source_db_adapter.connect(db_path=source_db_path)
    else:
        if not all([source_host, source_user, source_password, source_database]):
            raise ValueError("MySQL和PostgreSQL需要指定 source_host, source_user, source_password, source_database 参数")
        connect_params = {
            'host': source_host,
            'user': source_user,
            'password': source_password,
            'database': source_database
        }
        if source_port:
            connect_params['port'] = source_port
        source_db_adapter.connect(**connect_params)

    # 建立目标数据库连接
    if target_db_type == 'sqlite':
        if not target_db_path:
            # 如果未指定目标数据库路径，则使用源数据库路径
            if source_db_type == 'sqlite' and source_db_path:
                connect_params = {'db_path': source_db_path}
            else:
                raise ValueError("目标SQLite数据库需要指定 target_db_path 参数")
        else:
            connect_params = {'db_path': target_db_path}
        target_db_adapter.connect(**connect_params)
    else:
        if not all([target_host, target_user, target_password, target_database]):
            raise ValueError("目标MySQL和PostgreSQL需要指定 target_host, target_user, target_password, target_database 参数")
        connect_params = {
            'host': target_host,
            'user': target_user,
            'password': target_password,
            'database': target_database
        }
        if target_port:
            connect_params['port'] = target_port
        target_db_adapter.connect(**connect_params)

    # 创建对比器实例
    comparator = TableComparator(source_db_adapter, target_db_adapter)
    comparator.set_tables(table1, table2)
    
    if fields:
        comparator.set_fields(fields)
        
    if exclude:
        comparator.set_exclude_fields(exclude)
        
    if where:
        comparator.set_where_condition(where)
        
    # 添加对表特定WHERE条件的支持
    if where1:
        comparator.set_where_condition1(where1)
        
    if where2:
        comparator.set_where_condition2(where2)

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
    source_db_adapter.close()
    target_db_adapter.close()
    
    return result


if __name__ == "__main__":
    main()