#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# 添加上级目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入主模块
from table_diff import DMAdapter, get_database_adapter


class TestDMAdapter(unittest.TestCase):
    """测试达梦数据库适配器类"""

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_connection(self, mock_import_module):
        """测试达梦数据库适配器连接"""
        print("测试达梦数据库适配器连接...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        adapter = DMAdapter()
        connection = adapter.connect(
            host='localhost',
            port=5236,
            user='SYSDBA',
            password='password',
            database='TESTDB'
        )
        
        # 验证连接参数是否正确传递
        mock_dmPython.connect.assert_called_with(
            user='SYSDBA',
            password='password',
            server='localhost',
            port=5236,
            schema='TESTDB'
        )
        
        self.assertIsNotNone(connection)
        self.assertEqual(connection, mock_connection)
        print("测试达梦数据库适配器连接完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_connection_with_defaults(self, mock_import_module):
        """测试达梦数据库适配器使用默认参数连接"""
        print("测试达梦数据库适配器使用默认参数连接...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        adapter = DMAdapter()
        connection = adapter.connect(
            user='SYSDBA',
            password='password'
        )
        
        # 验证默认参数是否正确使用
        mock_dmPython.connect.assert_called_with(
            user='SYSDBA',
            password='password',
            server='localhost',
            port=5236,
            schema=None
        )
        
        self.assertIsNotNone(connection)
        print("测试达梦数据库适配器使用默认参数连接完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_get_table_fields(self, mock_import_module):
        """测试达梦数据库适配器获取表字段"""
        print("测试达梦数据库适配器获取表字段...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        # 模拟查询结果
        mock_cursor.fetchall.return_value = [
            ('ID',),
            ('NAME',),
            ('EMAIL',),
            ('AGE',)
        ]
        
        adapter = DMAdapter()
        adapter.connect(user='SYSDBA', password='password')
        
        fields = adapter.get_table_fields('test_table')
        
        # 验证执行的SQL语句
        mock_cursor.execute.assert_called_with(
            """
                    SELECT COLUMN_NAME 
                    FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = UPPER(?)
                    ORDER BY COLUMN_ID
                """, ('test_table',)
        )
        
        # 验证返回的字段
        self.assertEqual(fields, ['ID', 'NAME', 'EMAIL', 'AGE'])
        print("测试达梦数据库适配器获取表字段完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_get_table_fields_with_schema(self, mock_import_module):
        """测试达梦数据库适配器获取带模式的表字段"""
        print("测试达梦数据库适配器获取带模式的表字段...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        # 模拟查询结果
        mock_cursor.fetchall.return_value = [
            ('ID',),
            ('NAME',),
            ('EMAIL',)
        ]
        
        adapter = DMAdapter()
        adapter.connect(user='SYSDBA', password='password')
        
        fields = adapter.get_table_fields('schema.test_table')
        
        # 验证执行的SQL语句
        mock_cursor.execute.assert_called_with(
            """
                    SELECT COLUMN_NAME 
                    FROM ALL_TAB_COLUMNS 
                    WHERE TABLE_NAME = UPPER(?) AND OWNER = UPPER(?)
                    ORDER BY COLUMN_ID
                """, ('test_table', 'schema')
        )
        
        # 验证返回的字段
        self.assertEqual(fields, ['ID', 'NAME', 'EMAIL'])
        print("测试达梦数据库适配器获取带模式的表字段完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_get_primary_keys(self, mock_import_module):
        """测试达梦数据库适配器获取主键"""
        print("测试达梦数据库适配器获取主键...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        # 模拟查询结果
        mock_cursor.fetchall.return_value = [('ID',)]
        
        adapter = DMAdapter()
        adapter.connect(user='SYSDBA', password='password')
        
        primary_keys = adapter.get_primary_keys('test_table')
        
        # 验证执行的SQL语句
        mock_cursor.execute.assert_called()
        
        # 验证返回的主键
        self.assertEqual(primary_keys, ['ID'])
        print("测试达梦数据库适配器获取主键完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_execute_query(self, mock_import_module):
        """测试达梦数据库适配器执行查询"""
        print("测试达梦数据库适配器执行查询...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        adapter = DMAdapter()
        adapter.connect(user='SYSDBA', password='password')
        
        query = "SELECT * FROM test_table"
        cursor = adapter.execute_query(query)
        
        # 验证查询是否正确执行
        mock_cursor.execute.assert_called_with(query)
        self.assertEqual(cursor, mock_cursor)
        print("测试达梦数据库适配器执行查询完成")

    @patch('table_diff.importlib.import_module')
    def test_dm_adapter_close(self, mock_import_module):
        """测试达梦数据库适配器关闭连接"""
        print("测试达梦数据库适配器关闭连接...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        adapter = DMAdapter()
        adapter.connect(user='SYSDBA', password='password')
        
        # 执行关闭操作
        adapter.close()
        
        # 验证连接的close方法被调用
        mock_connection.close.assert_called_once()
        print("测试达梦数据库适配器关闭连接完成")

    def test_dm_adapter_import_error(self):
        """测试达梦数据库适配器导入错误"""
        print("测试达梦数据库适配器导入错误...")
        
        # 保存原始的sys.modules状态
        original_modules = dict(sys.modules)
        
        try:
            # 移除dmPython模块来模拟导入错误
            if 'dmPython' in sys.modules:
                del sys.modules['dmPython']
            
            # 重新导入table_diff，确保没有缓存的dmPython
            if 'table_diff' in sys.modules:
                del sys.modules['table_diff']
                
            # 重新导入模块
            import table_diff
            
            # 手动触发导入错误
            with patch('importlib.import_module', side_effect=ImportError("No module named 'dmPython'")):
                with self.assertRaises(ImportError) as context:
                    # 创建DMAdapter实例会触发importlib.import_module
                    table_diff.DMAdapter()
                
                # 验证错误消息
                self.assertIn("需要安装dmPython库", str(context.exception))
                print("测试达梦数据库适配器导入错误完成")
        finally:
            # 恢复sys.modules状态
            sys.modules.update(original_modules)

    @patch('table_diff.importlib.import_module')
    def test_get_database_adapter_for_dm(self, mock_import_module):
        """测试获取达梦数据库适配器"""
        print("测试获取达梦数据库适配器...")
        
        # 模拟dmPython模块
        mock_dmPython = Mock()
        mock_connection = Mock()
        mock_dmPython.connect.return_value = mock_connection
        mock_import_module.return_value = mock_dmPython
        
        dm_adapter = get_database_adapter('dm')
        self.assertIsInstance(dm_adapter, DMAdapter)
        
        # 测试连接是否正常工作
        connection = dm_adapter.connect(user='SYSDBA', password='password')
        self.assertIsNotNone(connection)
        print("测试获取达梦数据库适配器完成")


if __name__ == '__main__':
    unittest.main()