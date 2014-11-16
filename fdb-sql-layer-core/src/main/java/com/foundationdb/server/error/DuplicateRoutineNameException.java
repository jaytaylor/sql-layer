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

public final class DuplicateRoutineNameException extends InvalidOperationException {
    public DuplicateRoutineNameException(TableName name) {
        super(ErrorCode.DUPLICATE_ROUTINE, name.getSchemaName(), name.getTableName());
    }
    
    public DuplicateRoutineNameException(String schemaName, String routineName)
    {
        super(ErrorCode.DUPLICATE_ROUTINE, schemaName, routineName);
    }
}
