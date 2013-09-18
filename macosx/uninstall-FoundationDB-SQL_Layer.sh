#!/bin/bash

/bin/launchctl unload /Library/LaunchDaemons/com.foundationdb.layer.sql.plist &>/dev/null || :

rm -f /Library/LaunchDaemons/com.foundationdb.layer.sql.plist
rm -f /usr/local/bin/fdbsqldump
rm -f /usr/local/bin/fdbsqlload
rm -f /usr/local/libexec/fdbsqllayer
rm -f /usr/local/foundationdb/LICENSE-SQL_Layer
rm -f /usr/local/foundationdb/uninstall-FoundationDB-SQL_Layer.sh

rm -rf /usr/local/foundationdb/sql

/usr/sbin/pkgutil --forget com.foundationdb.layer.sql >/dev/null

echo "Your configuration and log files not been removed."
echo "To remove these files, delete the following directories:"
echo "  - /usr/local/foundationdb/logs/sql"
echo "  - /usr/local/etc/foundationdb/sql"

