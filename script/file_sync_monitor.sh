#!/bin/bash

###################
# 全局配置
###################

# 配置和日志文件路径
CONFIG_FILE="sync_config.ini"     # 配置文件：包含服务器信息和文件列表
SYNC_LOG="sync_history.log"       # 同步历史日志：记录成功同步的文件
ERROR_LOG="sync_error.log"        # 错误日志：记录同步失败的情况
SFTP_BATCH="/tmp/sftp_batch.txt"  # SFTP批处理命令文件

# 文件写入检测参数
MAX_WAIT_TIME=300                 # 等待文件写入完成的最大时间（秒）
CHECK_INTERVAL=2                  # 检查文件状态的时间间隔（秒）
STABLE_COUNT_REQUIRED=3           # 文件大小保持不变的次数，用于确认写入完成

# 文件检查方法
CHECK_METHOD="size_check"  # 默认使用文件大小检查

###################
# 工具函数
###################

# 获取文件的MD5值，用于判断文件是否发生变化
get_file_md5() {
    local file=$1
    md5sum "$file" | cut -d' ' -f1
}

# 记录错误信息到日志文件
log_error() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local filename=$1
    local filepath=$2
    local remote_path=$3
    local error_msg=$4
    
    echo "$timestamp|$filename|$filepath|$remote_path|$error_msg" >> "$ERROR_LOG"
    echo "错误已记录到: $ERROR_LOG"
}

# 记录成功同步的信息到历史日志
log_sync_history() {
    local filepath=$1
    local md5=$2
    local remote_path=$3
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "$timestamp|$filepath|$md5|$remote_path" >> "$SYNC_LOG"
}

###################
# 文件检查函数
###################

# 检查lsof命令是否可用
check_lsof_available() {
    command -v lsof >/dev/null 2>&1
}

# 使用lsof检查文件是否被写入
is_file_being_written_lsof() {
    local filepath=$1
    
    # 检查文件是否被以写入模式打开
    if lsof "$filepath" 2>/dev/null | grep -q "w"; then
        return 0  # 文件正在被写入
    fi
    return 1  # 文件未被写入
}

# 使用文件大小检查文件是否被写入
is_file_being_written_size() {
    local filepath=$1
    local size1=$(stat -c %s "$filepath" 2>/dev/null)
    sleep $CHECK_INTERVAL
    local size2=$(stat -c %s "$filepath" 2>/dev/null)
    
    [ "$size1" != "$size2" ]  # 如果大小不同，返回true（0）
}

# 统一的文件写入检查接口
is_file_being_written() {
    local filepath=$1
    
    case "$CHECK_METHOD" in
        "lsof_check")
            if check_lsof_available; then
                is_file_being_written_lsof "$filepath"
                return $?
            else
                echo "警告: lsof 不可用，切换到文件大小检查方法"
                CHECK_METHOD="size_check"
                is_file_being_written_size "$filepath"
                return $?
            fi
            ;;
        "size_check"|*)
            is_file_being_written_size "$filepath"
            return $?
            ;;
    esac
}

# 使用lsof等待文件写入完成
wait_for_file_completion_lsof() {
    local filepath=$1
    local wait_time=0
    
    echo "检测到文件正在写入，等待完成: $filepath"
    
    while [ $wait_time -lt $MAX_WAIT_TIME ]; do
        if ! is_file_being_written_lsof "$filepath"; then
            # 额外等待一小段时间确保写入完全结束
            sleep 3
            if ! is_file_being_written_lsof "$filepath"; then
                echo "文件写入已完成: $filepath"
                return 0
            fi
        fi
        
        wait_time=$((wait_time + CHECK_INTERVAL))
        echo "等待文件写入完成... ($wait_time/${MAX_WAIT_TIME}秒)"
        sleep $CHECK_INTERVAL
    done
    
    echo "错误: 等待文件写入超时 - $filepath"
    return 1
}

# 使用文件大小变化等待文件写入完成
wait_for_file_completion_size() {
    local filepath=$1
    local wait_time=0
    local stable_count=0
    local last_size=$(stat -c %s "$filepath" 2>/dev/null)
    
    echo "检测到文件正在写入，等待完成: $filepath"
    
    while [ $wait_time -lt $MAX_WAIT_TIME ]; do
        sleep $CHECK_INTERVAL
        local current_size=$(stat -c %s "$filepath" 2>/dev/null)
        
        if [ "$last_size" = "$current_size" ]; then
            stable_count=$((stable_count + 1))
            if [ $stable_count -ge $STABLE_COUNT_REQUIRED ]; then
                echo "文件写入已完成: $filepath"
                return 0
            fi
        else
            stable_count=0
            last_size=$current_size
        fi
        
        wait_time=$((wait_time + CHECK_INTERVAL))
        echo "等待文件写入完成... ($wait_time/${MAX_WAIT_TIME}秒)"
    done
    
    echo "错误: 等待文件写入超时 - $filepath"
    return 1
}

# 统一的文件写入完成等待接口
wait_for_file_completion() {
    local filepath=$1
    
    case "$CHECK_METHOD" in
        "lsof_check")
            if check_lsof_available; then
                wait_for_file_completion_lsof "$filepath"
                return $?
            else
                echo "警告: lsof 不可用，切换到文件大小检查方法"
                CHECK_METHOD="size_check"
                wait_for_file_completion_size "$filepath"
                return $?
            fi
            ;;
        "size_check"|*)
            wait_for_file_completion_size "$filepath"
            return $?
            ;;
    esac
}

# 检查文件是否已经同步过
# 通过比对MD5值判断文件是否发生变化
check_if_synced() {
    local filepath=$1
    local md5=$2
    
    [ -f "$SYNC_LOG" ] && grep -q "^[^|]*|$filepath|$md5|" "$SYNC_LOG"
}

# 检查远程文件是否存在
check_remote_file() {
    local remote_path=$1
    ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "[ -f '$remote_path' ]"
}

###################
# 文件同步函数
###################

# 准备同步文件列表
prepare_sync_list() {
    local -n _sync_list=$1  # 通过引用传递数组
    local filename=$2
    local filepath=$3
    local remote_path=$4
    local overwrite=$5
    
    # 1. 检查本地文件是否存在
    if [ ! -f "$filepath" ]; then
        log_error "$filename" "$filepath" "$remote_path" "本地文件不存在"
        return 1
    fi
    
    # 2. 等待文件写入完成（如果正在写入）
    if is_file_being_written "$filepath"; then
        if ! wait_for_file_completion "$filepath"; then
            log_error "$filename" "$filepath" "$remote_path" "等待文件写入完成超时"
            return 1
        fi
    fi
    
    # 3. 检查文件是否需要同步
    local md5=$(get_file_md5 "$filepath")
    if check_if_synced "$filepath" "$md5"; then
        echo "跳过: 文件未变化 - $filename"
        return 0
    fi
    
    # 4. 检查是否可以覆盖远程文件
    if check_remote_file "$remote_path"; then
        if [ "$overwrite" != "Y" ]; then
            log_error "$filename" "$filepath" "$remote_path" "远程文件已存在且未设置覆盖"
            return 1
        fi
        echo "远程文件存在，将被覆盖 - $remote_path"
    fi
    
    # 5. 添加到同步列表
    _sync_list+=("$filename" "$filepath" "$remote_path" "$md5")
    return 0
}

# 创建远程目录
create_remote_dirs() {
    local -n _sync_list=$1
    local dirs_created=()
    local remote_dir
    
    # 收集所有需要创建的远程目录
    for ((i=0; i<${#_sync_list[@]}; i+=4)); do
        remote_dir=$(dirname "${_sync_list[i+2]}")
        if [[ ! " ${dirs_created[@]} " =~ " ${remote_dir} " ]]; then
            dirs_created+=("$remote_dir")
        fi
    done
    
    # 批量创建远程目录
    if [ ${#dirs_created[@]} -gt 0 ]; then
        local cmd="mkdir -p"
        for dir in "${dirs_created[@]}"; do
            cmd+=" '$dir'"
        done
        if ! ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "$cmd"; then
            echo "错误: 无法创建远程目录"
            return 1
        fi
    fi
    return 0
}

# 执行批量同步
do_batch_sync() {
    local -n _sync_list=$1
    
    # 如果没有文件需要同步，直接返回
    if [ ${#_sync_list[@]} -eq 0 ]; then
        echo "没有文件需要同步"
        return 0
    fi
    
    # 创建远程目录
    if ! create_remote_dirs _sync_list; then
        return 1
    fi
    
    # 创建SFTP批处理命令文件
    > "$SFTP_BATCH"
    for ((i=0; i<${#_sync_list[@]}; i+=4)); do
        local filepath="${_sync_list[i+1]}"
        local remote_path="${_sync_list[i+2]}"
        echo "put \"$filepath\" \"$remote_path\"" >> "$SFTP_BATCH"
    done
    
    # 执行SFTP批量传输
    echo "开始批量传输文件..."
    if sftp -b "$SFTP_BATCH" -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST"; then
        # 记录成功传输的文件
        for ((i=0; i<${#_sync_list[@]}; i+=4)); do
            local filename="${_sync_list[i]}"
            local filepath="${_sync_list[i+1]}"
            local remote_path="${_sync_list[i+2]}"
            local md5="${_sync_list[i+3]}"
            log_sync_history "$filepath" "$md5" "$remote_path"
            echo "成功: 文件已同步 - $filename"
        done
        return 0
    else
        echo "错误: 批量传输失败"
        return 1
    fi
}

###################
# 主程序
###################

main() {
    # 1. 检查配置文件是否存在
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "错误: 配置文件 $CONFIG_FILE 不存在"
        exit 1
    fi
    
    # 2. 读取远程服务器配置
    eval "$(grep "^REMOTE_" "$CONFIG_FILE")"
    
    # 3. 检查必要的配置是否完整
    if [ -z "$REMOTE_HOST" ] || [ -z "$REMOTE_USER" ] || [ -z "$SSH_KEY" ]; then
        echo "错误: 缺少远程服务器配置"
        exit 1
    fi
    
    echo "开始文件同步..."
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "-----------------------------------"
    
    # 读取检查方法配置
    if grep -q "^CHECK_METHOD=" "$CONFIG_FILE"; then
        CHECK_METHOD=$(grep "^CHECK_METHOD=" "$CONFIG_FILE" | cut -d'=' -f2)
    fi
    
    # 验证检查方法
    case "$CHECK_METHOD" in
        "lsof_check")
            if ! check_lsof_available; then
                echo "警告: lsof 不可用，切换到文件大小检查方法"
                CHECK_METHOD="size_check"
            fi
            ;;
        "size_check")
            ;;
        *)
            echo "警告: 未知的检查方法 '$CHECK_METHOD'，使用默认的文件大小检查方法"
            CHECK_METHOD="size_check"
            ;;
    esac
    
    echo "使用文件检查方法: $CHECK_METHOD"
    
    # 4. 收集需要同步的文件
    declare -a sync_list=()
    
    while IFS='|' read -r filename filepath remote_path overwrite || [ -n "$filename" ]; do
        # 跳过注释和空行
        [[ $filename =~ ^#.*$ || -z "$filename" ]] && continue
        
        # 去除空格
        filename=${filename// /}
        filepath=${filepath// /}
        remote_path=${remote_path// /}
        overwrite=${overwrite// /}
        
        # 验证覆盖标志
        if [ "$overwrite" != "Y" ] && [ "$overwrite" != "N" ]; then
            log_error "$filename" "$filepath" "$remote_path" "无效的覆盖标志: $overwrite"
            continue
        fi
        
        # 准备同步文件
        prepare_sync_list sync_list "$filename" "$filepath" "$remote_path" "$overwrite"
    done < <(grep -v "^REMOTE_" "$CONFIG_FILE" | grep "|")
    
    # 5. 执行批量同步
    do_batch_sync sync_list
    
    echo "-----------------------------------"
    echo "同步完成"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # 清理临时文件
    rm -f "$SFTP_BATCH"
}

# 执行主程序
trap 'rm -f "$SFTP_BATCH"' EXIT
main 