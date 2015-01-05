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

package com.foundationdb.sql.aisddl;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.sql.parser.ExistenceCheck;

public class DDLHelper {
    private DDLHelper() {}

    public static TableName convertName(String defaultSchema, com.foundationdb.sql.parser.TableName parserName) {
        final String schema = parserName.hasSchema() ? parserName.getSchemaName() : defaultSchema;
        return new TableName(schema, parserName.getTableName());
    }

    public static boolean skipOrThrow(QueryContext context, ExistenceCheck check, Object o, InvalidOperationException e) {
        if(check == ExistenceCheck.IF_EXISTS) {
            if(o == null) {
                if(context != null) {
                    context.warnClient(e);
                }
                return true;
            }
            throw e;
        }
        if(check == ExistenceCheck.IF_NOT_EXISTS) {
            if(o != null) {
                if(context != null) {
                    context.warnClient(e);
                }
                return true;
            }
            throw e;
        }
        if((check == ExistenceCheck.NO_CONDITION) || (check == null)) {
            throw e;
        }
        throw new IllegalStateException("Unexpected condition: " + check);
    }
}
