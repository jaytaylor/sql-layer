#!/bin/bash

LAUNCH_PLIST="/Library/LaunchDaemons/com.foundationdb.layer.sql.plist"
LOG_DIR="/usr/local/foundationdb/logs/sql"
CONF_DIR="/usr/local/etc/foundationdb/sql"

if [ -e "${LAUNCH_PLIST}" ]; then
    /bin/launchctl unload "${LAUNCH_PLIST}" || :
fi

rm -f "${LAUNCH_PLIST}"
rm -f /usr/local/bin/fdbsql*
rm -f /usr/local/libexec/fdbsqllayer
rm -f /usr/local/foundationdb/LICENSE-SQL_Layer
rm -f /usr/local/foundationdb/LICENSE-SQL_LAYER_CLIENT_TOOLS
rm -f /usr/local/foundationdb/uninstall-FoundationDB-SQL_Layer.sh

rm -rf /usr/local/foundationdb/sql

/usr/sbin/pkgutil --forget com.foundationdb.layer.sql >/dev/null 2>&1
/usr/sbin/pkgutil --forget com.foundationdb.layer.sql.client.tools >/dev/null 2>&1

if [ -d "${LOG_DIR}" -o -d "${CONF_DIR}" ]; then
    echo "Your configuration and log files not been removed."
    echo "To remove these files, delete the following directories:"
    echo "  - ${LOG_DIR}"
    echo "  - ${CONF_DIR}"
else
    # Still only if empty
    rmdir /usr/local/foundationdb 2>/dev/null || :
fi

