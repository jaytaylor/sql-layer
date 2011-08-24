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

public class GroupHasMultipleRootsException extends InvalidOperationException {
    //Group `%s` has multiple root tables: `%s`.`%s` and `%s`.`%s`
    public GroupHasMultipleRootsException (String groupName, TableName table1, TableName table2) {
        super(ErrorCode.GROUP_MULTIPLE_ROOTS, groupName, 
                table1.getSchemaName(), table1.getTableName(),
                table2.getSchemaName(), table2.getTableName());
    }
}
