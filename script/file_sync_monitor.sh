#!/bin/bash

###################
# Global Configuration
###################

# Configuration and log file paths
CONFIG_FILE="sync_config.ini"     # Config file: contains server info and file list
SYNC_LOG="sync_history.log"       # Sync history log: records successfully synced files
ERROR_LOG="sync_error.log"        # Error log: records sync failures
SFTP_BATCH="/tmp/sftp_batch.txt"  # SFTP batch command file

# File write detection parameters
MAX_WAIT_TIME=120                 # Maximum wait time for file write completion (seconds)
CHECK_INTERVAL=0.1                  # Interval for checking file status (seconds)
STABLE_COUNT_REQUIRED=3           # Required count of stable size checks to confirm write completion

# SSH key path
SSH_KEY="/path/to/ssh/key"       # Default SSH key path

###################
# Utility Functions
###################

# Get file MD5 value to determine if file has changed
get_file_md5() {
    local file=$1
    md5sum "$file" | cut -d' ' -f1
}

# Log error message to error log file
log_error() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local local_dir=$(dirname "$2")
    local filename=$1
    local md5=$5        # New parameter for MD5
    local user=$6
    local host=$7
    local remote_path=$3
    local error_msg=$4
    
    # Format: timestamp|local_dir|filename|md5|user@host:remote_path|error_msg
    echo "$timestamp|$local_dir|$filename|$md5|$user@$host:$remote_path|$error_msg" >> "$ERROR_LOG"
    echo "Error logged to: $ERROR_LOG"
}

# Log successful sync to history log
log_sync_history() {
    local filepath=$1
    local md5=$2
    local remote_path=$3
    local user=$4
    local host=$5
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local local_dir=$(dirname "$filepath")
    local filename=$(basename "$filepath")
    
    # Format: timestamp|local_dir|filename|md5|user@host:remote_path
    echo "$timestamp|$local_dir|$filename|$md5|$user@$host:$remote_path" >> "$SYNC_LOG"
}

###################
# File Check Functions
###################

# Check if file is being written
is_file_being_written() {
    local filepath=$1
    local size1=$(stat -c %s "$filepath" 2>/dev/null)
    sleep $CHECK_INTERVAL
    local size2=$(stat -c %s "$filepath" 2>/dev/null)
    
    [ "$size1" != "$size2" ]  # Returns true (0) if size is different
}

# Wait for file write completion
wait_for_file_completion() {
    local filepath=$1
    local wait_time=0
    local stable_count=0
    local last_size=$(stat -c %s "$filepath" 2>/dev/null)
    
    echo "File is being written, waiting for completion: $filepath"
    
    while [ $wait_time -lt $MAX_WAIT_TIME ]; do
        sleep $CHECK_INTERVAL
        local current_size=$(stat -c %s "$filepath" 2>/dev/null)
        
        if [ "$last_size" = "$current_size" ]; then
            stable_count=$((stable_count + 1))
            if [ $stable_count -ge $STABLE_COUNT_REQUIRED ]; then
                echo "File write completed: $filepath"
                return 0
            fi
        else
            stable_count=0
            last_size=$current_size
        fi
        
        wait_time=$((wait_time + CHECK_INTERVAL))
        echo "Waiting for file write completion... ($wait_time/${MAX_WAIT_TIME}s)"
    done
    
    echo "Error: File write timeout - $filepath"
    return 1
}

# Check if file has already been synced
check_if_synced() {
    local filepath=$1
    local md5=$2
    local user=$3
    local host=$4
    local remote_path=$5
    local filename=$(basename "$filepath")
    local local_dir=$(dirname "$filepath")
    
    # Check both MD5 and complete remote path (user@host:remote_path)
    [ -f "$SYNC_LOG" ] && grep -q "^[^|]*|$local_dir|$filename|$md5|$user@$host:$remote_path$" "$SYNC_LOG"
}

# Find files matching pattern in directory
find_matching_files() {
    local pattern=$1
    local dir=$2
    find "$dir" -maxdepth 1 -type f -name "$pattern" 2>/dev/null
}

###################
# File Sync Functions
###################

# Check if remote file exists
check_remote_file() {
    local remote_path=$1
    local user=$2
    local host=$3
    ssh -i "$SSH_KEY" "$user@$host" "[ -f '$remote_path' ]"
}

# Create remote directories
create_remote_dirs() {
    local -n _sync_list=$1
    local user=$2
    local host=$3
    local dirs_created=()
    local remote_dir
    
    # Collect all required remote directories
    for ((i=0; i<${#_sync_list[@]}; i+=4)); do
        remote_dir=$(dirname "${_sync_list[i+2]}")
        if [[ ! " ${dirs_created[@]} " =~ " ${remote_dir} " ]]; then
            dirs_created+=("$remote_dir")
        fi
    done
    
    # Batch create remote directories
    if [ ${#dirs_created[@]} -gt 0 ]; then
        local cmd="mkdir -p"
        for dir in "${dirs_created[@]}"; do
            cmd+=" '$dir'"
        done
        if ! ssh -i "$SSH_KEY" "$user@$host" "$cmd"; then
            echo "Error: Failed to create remote directories"
            return 1
        fi
    fi
    return 0
}

# Prepare sync list for a specific host
prepare_host_sync_list() {
    local -n _sync_list=$1       # Pass array by reference
    local user=$2
    local host=$3
    local remote_base=$4
    local file_pattern=$5
    local local_dir=$6
    
    # Find all matching files
    while IFS= read -r filepath; do
        local filename=$(basename "$filepath")
        local remote_path="${remote_base}/${filename}"
        
        # Check if file is ready for sync
        if [ ! -f "$filepath" ]; then
            log_error "$filename" "$filepath" "$remote_path" "Local file does not exist" "" "$user" "$host"
            continue
        fi
        
        if is_file_being_written "$filepath"; then
            if ! wait_for_file_completion "$filepath"; then
                log_error "$filename" "$filepath" "$remote_path" "File write timeout" "" "$user" "$host"
                continue
            fi
        fi
        
        local md5=$(get_file_md5 "$filepath")
        # Pass remote_path to check_if_synced
        if check_if_synced "$filepath" "$md5" "$user" "$host" "$remote_path"; then
            echo "Skip: File unchanged and already synced to $user@$host:$remote_path - $filename"
            continue
        fi
        
        # Add to sync list
        _sync_list+=("$filename" "$filepath" "$remote_path" "$md5")
        
    done < <(find_matching_files "$file_pattern" "$local_dir")
}

# Sync files for a specific host
sync_host_files() {
    local user=$1
    local host=$2
    local -n _config_list=$3    # List of configurations for this host
    
    echo "Processing Host: $host (User: $user)"
    declare -a sync_list=()
    
    # Prepare sync list for all configurations of this host
    for ((i=0; i<${#_config_list[@]}; i+=3)); do
        local remote_dir="${_config_list[i]}"
        local file_pattern="${_config_list[i+1]}"
        local local_dir="${_config_list[i+2]}"
        
        prepare_host_sync_list sync_list "$user" "$host" "$remote_dir" "$file_pattern" "$local_dir"
    done
    
    # If there are files to sync, perform the sync
    if [ ${#sync_list[@]} -gt 0 ]; then
        echo "Found ${#sync_list[@]} files to sync for $host"
        
        # Create remote directories
        if ! create_remote_dirs sync_list "$user" "$host"; then
            log_error "N/A" "N/A" "N/A" "Failed to create remote directories" "N/A" "$user" "$host"
            return 1
        fi
        
        # Create SFTP batch command file
        > "$SFTP_BATCH"
        for ((i=0; i<${#sync_list[@]}; i+=4)); do
            local filepath="${sync_list[i+1]}"
            local remote_path="${sync_list[i+2]}"
            echo "put \"$filepath\" \"$remote_path\"" >> "$SFTP_BATCH"
        done
        
        # Execute SFTP batch transfer
        echo "Starting batch file transfer to $host..."
        if sftp -b "$SFTP_BATCH" -i "$SSH_KEY" "$user@$host"; then
            # Record successfully transferred files
            for ((i=0; i<${#sync_list[@]}; i+=4)); do
                local filename="${sync_list[i]}"
                local filepath="${sync_list[i+1]}"
                local remote_path="${sync_list[i+2]}"
                local md5="${sync_list[i+3]}"
                log_sync_history "$filepath" "$md5" "$remote_path" "$user" "$host"
                echo "Success: File synced - $filename"
            done
        else
            log_error "N/A" "N/A" "N/A" "Batch transfer failed" "N/A" "$user" "$host"
            return 1
        fi
    else
        echo "No files to sync for $host"
    fi
}

###################
# Main Program
###################

main() {
    # Check if config file exists
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "Error: Config file $CONFIG_FILE does not exist"
        exit 1
    fi
    
    echo "Starting file sync..."
    echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "-----------------------------------"
    
    # Group configurations by user and host
    declare -A host_configs
    while IFS='|' read -r user host remote_dir pattern local_dir || [ -n "$user" ]; do
        # Skip comments and empty lines
        [[ $user =~ ^#.*$ || -z "$user" ]] && continue
        
        # Remove spaces and newlines
        user=$(echo "$user" | tr -d '[:space:]')
        host=$(echo "$host" | tr -d '[:space:]')
        remote_dir=$(echo "$remote_dir" | tr -d '[:space:]')
        pattern=$(echo "$pattern" | tr -d '[:space:]')
        local_dir=$(echo "$local_dir" | tr -d '[:space:]')
        
        # Create unique key for this user and host combination
        local key="${user}:${host}"
        
        # Add configuration to array
        if [ -z "${host_configs[$key]}" ]; then
            host_configs[$key]="${remote_dir}|${pattern}|${local_dir}"
        else
            host_configs[$key]+=" ${remote_dir}|${pattern}|${local_dir}"
        fi
        
    done < "$CONFIG_FILE"
    
    # Process each user and host combination
    for key in "${!host_configs[@]}"; do
        IFS=':' read -r user host <<< "$key"
        
        # Convert configuration string to array
        declare -a config_list
        while IFS='|' read -r remote_dir pattern local_dir; do
            config_list+=("$remote_dir" "$pattern" "$local_dir")
        done < <(echo "${host_configs[$key]}" | tr ' ' '\n')
        
        # Sync files for this host
        sync_host_files "$user" "$host" config_list
    done
    
    echo "-----------------------------------"
    echo "Sync completed"
    echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # Clean up temporary files
    rm -f "$SFTP_BATCH"
}

# Execute main program
trap 'rm -f "$SFTP_BATCH"' EXIT
main 