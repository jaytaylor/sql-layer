/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.error;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.unparser.NodeToString;

public class BaseSQLException extends InvalidOperationException 
{
    protected int errorPosition;

    protected BaseSQLException(ErrorCode code, String msg, int errorPosition) {
        super(code, msg);
        this.errorPosition = errorPosition;
    }

    protected BaseSQLException(ErrorCode code, QueryTreeNode sql) {
        super(code, formatSQL(sql));
        this.errorPosition = getErrorPosition(sql);
    }

    protected BaseSQLException(ErrorCode code, String msg, QueryTreeNode sql) {
        super(code, msg, formatSQL(sql));
        this.errorPosition = getErrorPosition(sql);
    }
    
    protected BaseSQLException(ErrorCode code, String msg1, String msg2, QueryTreeNode sql) {
        super(code, msg1, msg2, formatSQL(sql));
        this.errorPosition = getErrorPosition(sql);
    }
    
    public int getErrorPosition() {
        return errorPosition;
    }

    protected static int getErrorPosition(QueryTreeNode sql) {
        return (sql == null) ? 0 : sql.getBeginOffset() + 1;
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
