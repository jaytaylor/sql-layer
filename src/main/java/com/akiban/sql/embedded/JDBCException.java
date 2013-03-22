
package com.akiban.sql.embedded;

import com.akiban.server.error.InvalidOperationException;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class JDBCException extends SQLException
{
    public JDBCException() {
        super();
    }

    public JDBCException(String reason) {
        super(reason);
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
    private static class Wrapper extends RuntimeException {
        public Wrapper(SQLException cause) {
            super(cause);
        }
    }

    protected static RuntimeException wrapped(String reason) {
        return new Wrapper(new JDBCException(reason));
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
