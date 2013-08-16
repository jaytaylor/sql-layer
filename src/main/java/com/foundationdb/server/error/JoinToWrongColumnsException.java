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

import com.foundationdb.ais.model.TableName;

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
