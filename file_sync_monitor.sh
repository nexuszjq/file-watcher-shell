#!/bin/bash

###################
# 全局配置
###################

# 配置和日志文件路径
CONFIG_FILE="sync_config.ini"     # 配置文件：包含服务器信息和文件列表
SYNC_LOG="sync_history.log"       # 同步历史日志：记录成功同步的文件
ERROR_LOG="sync_error.log"        # 错误日志：记录同步失败的情况

# 文件写入检测参数
MAX_WAIT_TIME=300                 # 等待文件写入完成的最大时间（秒）
CHECK_INTERVAL=2                  # 检查文件状态的时间间隔（秒）
STABLE_COUNT_REQUIRED=3           # 文件大小保持不变的次数，用于确认写入完成

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
# 文件状态检查函数
###################

# 检查文件是否正在被写入
# 通过比较两次文件大小来判断
is_file_being_written() {
    local filepath=$1
    local size1=$(stat -c %s "$filepath" 2>/dev/null)
    sleep $CHECK_INTERVAL
    local size2=$(stat -c %s "$filepath" 2>/dev/null)
    
    [ "$size1" != "$size2" ]  # 如果大小不同，返回true（0）
}

# 等待文件写入完成
# 当文件大小连续多次保持不变时，认为写入完成
wait_for_file_completion() {
    local filepath=$1
    local wait_time=0
    local stable_count=0
    local last_size=$(stat -c %s "$filepath" 2>/dev/null)
    
    echo "检测到文件正在写入，等待完成: $filepath"
    
    while [ $wait_time -lt $MAX_WAIT_TIME ]; do
        sleep $CHECK_INTERVAL
        local current_size=$(stat -c %s "$filepath" 2>/dev/null)
        
        if [ "$last_size" = "$current_size" ]; then
            # 文件大小未变化，增加稳定计数
            stable_count=$((stable_count + 1))
            if [ $stable_count -ge $STABLE_COUNT_REQUIRED ]; then
                echo "文件写入已完成: $filepath"
                return 0
            fi
        else
            # 文件大小发生变化，重置稳定计数
            stable_count=0
            last_size=$current_size
        fi
        
        wait_time=$((wait_time + CHECK_INTERVAL))
        echo "等待文件写入完成... ($wait_time/${MAX_WAIT_TIME}秒)"
    done
    
    echo "错误: 等待文件写入超时 - $filepath"
    return 1
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

# 同步单个文件到远程服务器
sync_file() {
    local filename=$1
    local filepath=$2
    local remote_path=$3
    local overwrite=$4
    
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
    
    # 3. 检查文件是否需要同步（是否已同步且未修改）
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
    
    # 5. 创建远程目录
    local remote_dir=$(dirname "$remote_path")
    if ! ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "mkdir -p '$remote_dir'"; then
        log_error "$filename" "$filepath" "$remote_path" "无法创建远程目录"
        return 1
    fi
    
    # 6. 传输文件
    echo "正在同步: $filename -> $REMOTE_HOST:$remote_path"
    if sftp -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" <<EOF
        put "$filepath" "$remote_path"
EOF
    then
        log_sync_history "$filepath" "$md5" "$remote_path"
        echo "成功: 文件已同步 - $filename"
        return 0
    else
        log_error "$filename" "$filepath" "$remote_path" "SFTP传输失败"
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
    
    # 4. 读取并处理每个需要同步的文件
    grep -v "^REMOTE_" "$CONFIG_FILE" | grep "|" | while IFS='|' read -r filename filepath remote_path overwrite || [ -n "$filename" ]; do
        # 跳过注释和空行
        [[ $filename =~ ^#.*$ || -z "$filename" ]] && continue
        
        # 去除每个字段的空格
        filename=${filename// /}
        filepath=${filepath// /}
        remote_path=${remote_path// /}
        overwrite=${overwrite// /}
        
        # 验证覆盖标志的有效性
        if [ "$overwrite" != "Y" ] && [ "$overwrite" != "N" ]; then
            log_error "$filename" "$filepath" "$remote_path" "无效的覆盖标志: $overwrite"
            continue
        fi
        
        # 同步文件
        sync_file "$filename" "$filepath" "$remote_path" "$overwrite"
    done
    
    echo "-----------------------------------"
    echo "同步完成"
    echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
}

# 执行主程序
main 