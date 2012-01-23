/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */
package com.akiban.server.error;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.QueryTreeNode;
import com.akiban.sql.unparser.NodeToString;

public class BaseSQLException extends InvalidOperationException 
{
    protected int errorPosition;

    protected BaseSQLException(ErrorCode code, String msg, int errorPosition) {
        super(code, msg);
        this.errorPosition = errorPosition;
    }

    protected BaseSQLException(ErrorCode code, QueryTreeNode sql) {
        super(code, formatSQL(sql));
        if (sql != null)
            errorPosition = sql.getBeginOffset() + 1;
    }

    protected BaseSQLException(ErrorCode code, String msg, QueryTreeNode sql) {
        super(code, msg, formatSQL(sql));
        if (sql != null)
            errorPosition = sql.getBeginOffset() + 1;
    }
    
    protected BaseSQLException(ErrorCode code, String msg1, String msg2, QueryTreeNode sql) {
        super(code, msg1, msg2, formatSQL(sql));
        if (sql != null)
            errorPosition = sql.getBeginOffset() + 1;
    }
    
    public int getErrorPosition() {
        return errorPosition;
    }

    protected static String formatSQL(QueryTreeNode sql) {
        if (sql == null)
            return null;
        try {
            return new NodeToString().toString(sql);
        }
        catch (StandardException ex) {
            return sql.toString();
        }
    }
}
