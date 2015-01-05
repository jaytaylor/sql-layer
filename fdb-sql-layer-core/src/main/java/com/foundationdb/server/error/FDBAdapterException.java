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

import com.foundationdb.FDBException;

public class FDBAdapterException extends StoreAdapterRuntimeException
{
    /** For use with errors originating from the FDB client. */
    public FDBAdapterException(FDBException ex) {
        this(ErrorCode.FDB_ERROR, ex);
    }

    /** For use with FDB-specific SQL Layer errors that do not originate from the FDB client. */
    public FDBAdapterException(String msg) {
        this(ErrorCode.FDB_ERROR, msg);
    }

    protected FDBAdapterException(ErrorCode errorCode, FDBException ex) {
        this(errorCode, String.format("%d - %s", ex.getCode(), ex.getMessage()));
        initCause(ex);
    }

    protected FDBAdapterException(ErrorCode errorCode, String desc) {
        super(errorCode, desc);
    }
    
    protected FDBAdapterException (ErrorCode code, Long i, Long j, Long k, Long l) {
        super (code, i, j, k, l);
    }
}
