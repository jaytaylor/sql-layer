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

public class GeneratorWrongDatatypeException extends InvalidOperationException {
    // Table `{0}`.`{1}` has column `{2}` with an unsupported data type `{3}` for using a generator
    public GeneratorWrongDatatypeException(TableName table, String columnName, String type) {
        this(table.getSchemaName(), table.getTableName(), columnName, type);
    }

    public GeneratorWrongDatatypeException(String schemaName, String tableName, String columnName, String type) {
        super(ErrorCode.GENERATOR_WRONG_DATATYPE, schemaName, tableName, columnName, type);
    }
}
