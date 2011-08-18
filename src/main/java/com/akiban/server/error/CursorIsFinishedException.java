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

import com.akiban.server.api.dml.scan.CursorId;

public final class CursorIsFinishedException extends InvalidOperationException {
    //Finished scan cursor requested more rows: %s
    public CursorIsFinishedException(CursorId cursor) {
        super(ErrorCode.CURSOR_IS_FINISHED, cursor);
    }
}
