
package com.akiban.sql.embedded;

import java.sql.SQLWarning;

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.ErrorCode;

public class JDBCWarning extends SQLWarning
{
    public JDBCWarning(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
        super(message, errorCode.getFormattedValue());
    }
}
