#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import sys
import os
import argparse

def run_command(command, cwd=None):
    """运行命令并返回结果"""
    try:
        result = subprocess.run(command, shell=True, cwd=cwd, 
                              capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"命令执行失败: {command}")
        print(f"错误输出: {e.stderr}")
        return None

def git_status(cwd):
    """检查Git状态"""
    return run_command("git status --porcelain", cwd)

def git_add_all_except_json(cwd):
    """添加所有文件到Git暂存区，但排除JSON配置文件"""
    # 添加所有文件
    run_command("git add .", cwd)
    
    # 移除所有JSON文件
    json_files = run_command("git ls-files '*.json'", cwd)
    if json_files:
        for json_file in json_files.split('\n'):
            if json_file.strip():
                run_command(f"git reset HEAD \"{json_file.strip()}\"", cwd)
                print(f"已排除JSON文件: {json_file.strip()}")
    
    # 特别确保常见的配置文件不被提交
    config_files = [
        "diff_conf.json",
        "diff_conf_2.json", 
        "diff_conf_mssql_example.json",
        "diff_conf_oracle_example.json"
    ]
    
    for config_file in config_files:
        if os.path.exists(os.path.join(cwd, config_file)):
            run_command(f"git reset HEAD \"{config_file}\"", cwd)
            print(f"已排除配置文件: {config_file}")

def git_commit(cwd, message):
    """提交更改"""
    return run_command(f'git commit -m "{message}"', cwd)

def git_push(cwd, remote, branch="main"):
    """推送到远程仓库"""
    return run_command(f"git push {remote} {branch}", cwd)

def check_remotes(cwd):
    """检查远程仓库"""
    remotes = run_command("git remote -v", cwd)
    if remotes:
        print("检测到以下远程仓库:")
        print(remotes)
        return True
    else:
        print("未检测到远程仓库")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="提交代码变更到GitHub和Gitee（排除JSON文件）")
    parser.add_argument("message", nargs="*", help="提交信息")
    parser.add_argument("--no-push", action="store_true", help="只提交不推送")
    parser.add_argument("--github-only", action="store_true", help="只推送到GitHub")
    parser.add_argument("--gitee-only", action="store_true", help="只推送到Gitee")
    
    args = parser.parse_args()
    
    # 获取当前目录
    current_dir = os.getcwd()
    
    # 检查Git状态
    status = git_status(current_dir)
    if not status:
        print("没有需要提交的更改")
        return 0
    
    print("检测到以下未提交的更改:")
    print(status)
    
    # 获取提交信息
    if args.message:
        commit_message = " ".join(args.message)
    else:
        commit_message = input("请输入提交信息: ")
    
    if not commit_message.strip():
        print("提交信息不能为空")
        return 1
    
    # 添加文件（排除JSON文件）
    print("\n正在添加文件到暂存区（排除JSON文件）...")
    git_add_all_except_json(current_dir)
    
    # 检查是否有文件被添加到暂存区
    staged_files = run_command("git diff --cached --name-only", current_dir)
    if not staged_files:
        print("没有文件需要提交（JSON文件和其他配置文件已被排除）")
        return 0
    
    print("将提交以下文件:")
    print(staged_files)
    
    # 提交更改
    print("\n正在提交更改...")
    commit_result = git_commit(current_dir, commit_message)
    if commit_result is None:
        print("提交失败")
        return 1
    
    print("提交成功")
    
    # 如果指定了--no-push，则不推送
    if args.no_push:
        print("已跳过推送步骤")
        return 0
    
    # 检查远程仓库
    if not check_remotes(current_dir):
        print("无法推送，因为没有配置远程仓库")
        return 1
    
    # 推送到远程仓库
    if not args.gitee_only:
        print("\n正在推送到GitHub...")
        push_github = git_push(current_dir, "origin")
        if push_github is None:
            print("推送到GitHub失败")
        else:
            print("已成功推送到GitHub")
    
    if not args.github_only:
        # 检查是否存在gitee远程仓库
        remotes = run_command("git remote", current_dir)
        if remotes and "gitee" in remotes:
            print("\n正在推送到Gitee...")
            push_gitee = git_push(current_dir, "gitee")
            if push_gitee is None:
                print("推送到Gitee失败")
            else:
                print("已成功推送到Gitee")
        else:
            print("\n未检测到Gitee远程仓库，跳过推送")
    
    print("\n所有操作完成!")
    return 0

if __name__ == "__main__":
    sys.exit(main())