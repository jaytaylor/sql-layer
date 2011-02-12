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

package com.akiban.server.api.common;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.DMLException;
import com.akiban.server.util.RowDefNotFoundException;
import com.akiban.message.ErrorCode;

public final class NoSuchTableException extends DMLException {
    public NoSuchTableException(InvalidOperationException e) {
        super(e);
    }

    public NoSuchTableException(int tableId, RowDefNotFoundException e) {
        super(ErrorCode.NO_SUCH_TABLE, "TableId not found: " + tableId, e);
    }

    public NoSuchTableException(int id) {
        super(ErrorCode.NO_SUCH_TABLE, "No table with id %d", id);
    }

    public NoSuchTableException(TableName name) {
        super(ErrorCode.NO_SUCH_TABLE, "No table with name %s", name);
    }
}
