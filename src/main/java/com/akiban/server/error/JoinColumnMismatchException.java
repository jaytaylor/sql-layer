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

import com.akiban.ais.model.TableName;

public class JoinColumnMismatchException extends InvalidOperationException {
    //Join column list size (%d) for table `%s`.`%s` does not match table `%s`.`%s` primary key (%d)
    public JoinColumnMismatchException (int listSize, 
            TableName childTable, 
            TableName parentTable,
            int pkSize) {
        super (ErrorCode.JOIN_COLUMN_MISMATCH, listSize, childTable.getSchemaName(), childTable.getTableName(),
                parentTable.getSchemaName(), parentTable.getTableName(), pkSize);
    }
}
