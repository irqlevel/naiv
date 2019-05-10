#!/bin/bash
cat << END
[identity]
    uuid = "$IDENTITY_UUID"
    host = "$IDENTITY_HOST"

[cluster]
    hosts = $CLUSTER_HOSTS

[log]
    file_name = "$LOG_FILE_NAME"
END