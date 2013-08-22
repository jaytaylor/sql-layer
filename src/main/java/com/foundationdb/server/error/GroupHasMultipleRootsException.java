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

public class GroupHasMultipleRootsException extends InvalidOperationException {
    public GroupHasMultipleRootsException (TableName groupName, TableName table1, TableName table2) {
        super(ErrorCode.GROUP_MULTIPLE_ROOTS,
              groupName.getSchemaName(), groupName.getTableName(),
              table1.getSchemaName(), table1.getTableName(),
              table2.getSchemaName(), table2.getTableName());
    }
}
