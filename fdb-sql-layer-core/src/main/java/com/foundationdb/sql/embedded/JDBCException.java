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

package com.foundationdb.sql.embedded;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class JDBCException extends SQLException
{
    public JDBCException() {
        super();
    }

    public JDBCException(String reason, ErrorCode code) {
        super(reason, code.getFormattedValue());
    }
    
    public JDBCException(InvalidOperationException cause) {
        super(cause.getShortMessage(), cause.getCode().getFormattedValue(), cause);
    }
    
    public JDBCException(String reason, InvalidOperationException cause) {
        super(reason, cause.getCode().getFormattedValue(), cause);
    }
    
    public JDBCException(Throwable cause) {
        super(cause);
    }

    public JDBCException(String reason, Throwable cause) {
        super(reason, cause);
    }

    // Allow outer layer to throw SQLException through inner layer that does not.
    protected static class Wrapper extends RuntimeException {
        public Wrapper(SQLException cause) {
            super(cause);
        }
    }

    protected static RuntimeException wrapped(String reason, ErrorCode code) {
        return new Wrapper(new JDBCException(reason, code));
    }

    protected static RuntimeException throwUnwrapped(RuntimeException ex) throws SQLException {
        if (ex instanceof Wrapper)
            throw (SQLException)ex.getCause();
        if (ex instanceof InvalidOperationException)
            throw new JDBCException((InvalidOperationException)ex);
        if (ex instanceof UnsupportedOperationException)
            throw new SQLFeatureNotSupportedException(ex.getMessage());
        return ex;
    }
}
