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

public class JoinColumnTypesMismatchException extends InvalidOperationException {
    //Column `%s`.`%s`.`%s` type used for join does not match parent column `%s`.`%s`.`%s`
    public JoinColumnTypesMismatchException (TableName childTable, String childColumn, 
            TableName parentTable, String parentColumn) {
        super(ErrorCode.JOIN_TYPE_MISMATCH,
                childTable.getSchemaName(), childTable.getTableName(), childColumn,
                parentTable.getSchemaName(), parentTable.getTableName(), parentColumn);
    }
}
