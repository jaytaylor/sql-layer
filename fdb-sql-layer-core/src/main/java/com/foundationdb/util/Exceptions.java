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

package com.foundationdb.util;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.InvalidOperationException;

import java.sql.SQLException;
import java.util.List;

public final class Exceptions {
    /**
     * Throws the given throwable, downcast, if it's of the appropriate type
     *
     * @param t the exception to check
     * @param cls  the class to check for and cast to
     * @throws T the e instance, cast down
     */
    public static <T extends Throwable> void throwIfInstanceOf(Throwable t, Class<T> cls) throws T {
        if (cls.isInstance(t)) {
            throw cls.cast(t);
        }
    }

    /**
     * <p>Always throws something. If {@code t} is a RuntimeException, it simply gets thrown. If it's a checked
     * exception, it gets wrapped in a RuntimeException. If it's an Error, it simply gets thrown. And if somehow
     * it's something else, that thing is wrapped in an Error and thrown.</p>
     *
     * <p>The return value of Error is simply as a convenience for methods that return a non-void type; you can
     * invoke {@code throw throwAlways(t);} to indicate to the compiler that the method's control will end there.</p>
     * @param t the throwable to throw, possibly wrapped if needed
     * @return nothing, since something is always thrown from this method.
     */
    public static Error throwAlways(Throwable t) {
        throwIfInstanceOf(t,RuntimeException.class);
        if (t instanceof Exception) {
            throw new RuntimeException(t);
        }
        throwIfInstanceOf(t, Error.class);
        throw new Error("not a RuntimeException, checked exception or Error?!", t);
    }

    public static Error throwAlways(List<? extends Throwable> throwables, int index) {
        Throwable t = throwables.get(index);
        throw throwAlways(t);
    }

    public static boolean isRollbackException(Throwable t) {
        if(t instanceof SQLException) {
            String sqlState = ((SQLException)t).getSQLState();
            try {
                ErrorCode code = ErrorCode.valueOfCode(sqlState);
                return code.isRollbackClass();
            } catch(IllegalArgumentException e) {
                // Not a valid SQLState
                return false;
            }
        }
        return (t instanceof InvalidOperationException) &&
               ((InvalidOperationException)t).getCode().isRollbackClass();
    }
}
