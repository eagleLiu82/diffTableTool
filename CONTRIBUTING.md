# 贡献指南

感谢您考虑为数据库表对比工具做出贡献！我们欢迎各种形式的贡献，包括但不限于：

- 报告bug
- 提交修复
- 添加新功能
- 改进文档
- 添加测试用例

## 如何贡献

### 报告Bug

如果您发现了bug，请在GitHub上创建一个issue，包含以下信息：

1. 清晰的bug描述
2. 复现步骤
3. 预期行为
4. 实际行为
5. 环境信息（操作系统、Python版本、数据库类型及版本等）

### 提交代码

1. Fork本仓库
2. 创建您的特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启一个Pull Request

### 测试

为了确保代码质量，我们要求所有贡献都包含适当的测试：

1. 为新功能添加单元测试
2. 确保所有现有测试通过
3. 对于涉及性能改进的更改，请添加专门的性能测试
4. 测试文件应放在 `tests/` 目录下
5. 使用命名约定 `test_feature_name.py` 为测试文件命名

运行测试：
```
# 运行所有测试
cd tests && python run_tests.py

# 运行特定测试
cd tests && python test_streaming_comparison.py
```

### 代码规范

- 遵循PEP 8编码规范
- 添加适当的注释和文档字符串
- 确保所有测试通过
- 添加新的测试用例以覆盖新功能

### 运行测试

在提交代码前，请确保所有测试都能通过：

```bash
# 运行所有测试
python tests/run_tests.py

# 或者使用pytest（如果已安装）
pytest tests/
```

## 开发环境设置

1. 克隆仓库：
   ```bash
   git clone https://github.com/your-username/table-diff-tool.git
   cd table-diff-tool
   ```

2. 创建虚拟环境：
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # 或
   venv\Scripts\activate  # Windows
   ```

3. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

4. 运行测试确保环境正常：
   ```bash
   python tests/run_tests.py
   ```

## 提问

如果您有任何问题，可以在GitHub上创建issue，我们会尽快回复您。

再次感谢您的贡献！