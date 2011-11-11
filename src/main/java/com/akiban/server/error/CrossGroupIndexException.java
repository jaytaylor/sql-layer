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

public class CrossGroupIndexException extends InvalidOperationException {
    public CrossGroupIndexException(String indexName, String group1, TableName table1, String group2, TableName table2) {
        super(ErrorCode.CROSS_GROUP_INDEX, indexName, 
              group1, table1.getSchemaName(), table1.getTableName(), 
              group2, table2.getSchemaName(), table2.getTableName());
    }

}
