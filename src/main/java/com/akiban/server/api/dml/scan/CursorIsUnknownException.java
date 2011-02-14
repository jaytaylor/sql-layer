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

package com.akiban.server.api.dml.scan;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.DMLException;
import com.akiban.message.ErrorCode;

public final class CursorIsUnknownException extends DMLException {

    public CursorIsUnknownException(int tableId) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, "No cursor for tableId %s", tableId);
    }

    public CursorIsUnknownException(CursorId cursor) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, "Unknown cursor: %s", cursor);
    }

    public CursorIsUnknownException(InvalidOperationException e) {
        super(e);
    }
}
