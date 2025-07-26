# 更新日志

所有重要的项目更改都会记录在这个文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
版本遵循 [Semantic Versioning](https://semver.org/spec/v2.0.0.html)。

## [Unreleased]

### 添加
- GitHub Actions CI/CD 工作流
- 贡献指南文档
- 更新日志文件
- CSV格式详细差异报告功能
- 简化命令行工具使用方式 (`table_diff` 命令)

### 修复
- PostgreSQL 字段获取问题
- 改进了错误处理和日志记录
- 修复了对比结果中未包含所有行差异的问题
- 修复了PostgreSQL中查询结果顺序不一致导致的差异比较问题

### 更改
- 改进了对比逻辑以收集所有行差异用于CSV报告生成
- 改进了所有数据库类型的查询排序逻辑，确保结果一致性

## [1.0.0] - 2023-XX-XX

### 添加
- 支持 SQLite、MySQL 和 PostgreSQL 数据库
- 表字段对比功能
- 行数据对比功能
- 支持指定字段或排除字段
- 支持 WHERE 条件过滤
- 详细的差异报告
- 完整的测试套件
- 详细的文档说明

### 改进
- PostgreSQL 查询顺序处理
- 字段一致性检查
- 增强的调试日志支持