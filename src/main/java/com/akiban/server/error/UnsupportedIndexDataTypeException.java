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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;

public class UnsupportedIndexDataTypeException extends InvalidOperationException {
    //Table `%s`.`%s` index `%s` has unsupported type `%s` from column `%s`
    public UnsupportedIndexDataTypeException (TableName table, String index, String columnName, String typeName) {
        super (ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE, 
                table.getSchemaName(), table.getTableName(),
                index,
                typeName,
                columnName);
    }
}
