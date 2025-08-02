#!/bin/bash

# 脚本功能：提交代码变更到GitHub和Gitee（排除JSON配置文件）

set -e  # 遇到错误时退出

# 检查是否有未提交的更改
if [[ -z $(git status --porcelain) ]]; then
  echo "没有需要提交的更改"
  exit 0
fi

echo "检测到以下未提交的更改:"
git status --porcelain

# 获取提交信息
COMMIT_MSG=""
if [[ $# -gt 0 ]]; then
  COMMIT_MSG="$*"
else
  read -p "请输入提交信息: " COMMIT_MSG
fi

if [[ -z "$COMMIT_MSG" ]]; then
  echo "提交信息不能为空"
  exit 1
fi

# 添加所有文件
echo "正在添加文件到暂存区..."
git add .

# 移除JSON文件
echo "正在排除JSON配置文件..."
JSON_FILES=$(git ls-files '*.json')
if [[ -n "$JSON_FILES" ]]; then
  echo "$JSON_FILES" | while read -r file; do
    if [[ -n "$file" ]]; then
      git reset HEAD "$file"
      echo "已排除JSON文件: $file"
    fi
  done
fi

# 特别排除常见的配置文件
CONFIG_FILES=(
  "diff_conf.json"
  "diff_conf_2.json"
  "diff_conf_mssql_example.json"
  "diff_conf_oracle_example.json"
)

for config_file in "${CONFIG_FILES[@]}"; do
  if [[ -f "$config_file" ]]; then
    git reset HEAD "$config_file"
    echo "已排除配置文件: $config_file"
  fi
done

# 检查是否有文件被添加到暂存区
STAGED_FILES=$(git diff --cached --name-only)
if [[ -z "$STAGED_FILES" ]]; then
  echo "没有文件需要提交（JSON文件和其他配置文件已被排除）"
  exit 0
fi

echo "将提交以下文件:"
echo "$STAGED_FILES"

# 提交更改
echo "正在提交更改..."
git commit -m "$COMMIT_MSG"
echo "提交成功: $COMMIT_MSG"

# 推送到远程仓库
echo "检测到以下远程仓库:"
git remote -v

PUSH_COUNT=0

# 推送到GitHub
echo "正在推送到GitHub..."
if git push origin main; then
  echo "已成功推送到GitHub"
  ((PUSH_COUNT++))
else
  echo "推送到GitHub失败"
fi

# 推送到Gitee（如果存在）
if git remote | grep -q "gitee"; then
  echo "正在推送到Gitee..."
  if git push gitee main; then
    echo "已成功推送到Gitee"
    ((PUSH_COUNT++))
  else
    echo "推送到Gitee失败"
  fi
else
  echo "未检测到Gitee远程仓库，跳过推送"
fi

if [[ $PUSH_COUNT -gt 0 ]]; then
  echo "成功推送到 $PUSH_COUNT 个远程仓库!"
else
  echo "没有成功推送到任何远程仓库!"
fi

echo "所有操作完成!"