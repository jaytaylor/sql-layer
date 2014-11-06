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

import com.foundationdb.ais.model.IndexName;

public class DuplicateIndexIdException extends InvalidOperationException {
    public DuplicateIndexIdException (IndexName name1, IndexName name2) {
        super (ErrorCode.DUPLICATE_INDEXID, 
                name1.getSchemaName(), name1.getTableName(), name1.getName(),
                name2.getSchemaName(), name2.getTableName(), name2.getName());
    }
}
