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

public final class JoinToWrongColumnsException extends InvalidOperationException {
    //Table `%s`.`%s` join reference part `%s` does not match `%s`.`%s` primary key part `%s`

    public JoinToWrongColumnsException(TableName table, String columnName, TableName joinTo, String joinToColumn) {
    super(ErrorCode.JOIN_TO_WRONG_COLUMNS, 
            table.getSchemaName(), table.getTableName(),
            columnName, 
            joinTo.getSchemaName(), joinTo.getTableName(), 
            joinToColumn);
    }
}
