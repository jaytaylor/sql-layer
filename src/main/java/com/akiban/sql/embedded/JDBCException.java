/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
    }

    public JDBCException(String reason, Throwable cause) {
    }

    // Allow outer layer to throw SQLException throw inner layer that does not.
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
            throw new JDBCException(ex);
        if (ex instanceof UnsupportedOperationException)
            throw new SQLFeatureNotSupportedException(ex.getMessage());
        return ex;
    }
}
