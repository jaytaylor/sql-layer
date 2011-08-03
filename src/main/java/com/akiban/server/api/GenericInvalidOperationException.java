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

package com.akiban.server.api;

import com.akiban.server.error.InvalidOperationException;

/**
 * Wrapper for an InvalidOperationException. The sole purpose of this exception type is so that if you want to
 * explicitly list a few InvalidOperationException subclasses in your "throws" clause, but you also have to account for
 * a generic InvalidOperationException (for legacy reasons), you can use this instead of the plain
 * InvalidOperationException. Otherwise, that InvalidOperationException throws declaration makes the subclasses
 * redundant. 
 */
public final class GenericInvalidOperationException extends InvalidOperationException {
    private final InvalidOperationException cause;

    public GenericInvalidOperationException(InvalidOperationException e) {
        super(e.getCode(), e.getShortMessage(), e);
        this.cause = e;
    }

    public GenericInvalidOperationException(Throwable t) {
        super(t);
        this.cause = (t instanceof InvalidOperationException)
                ? (InvalidOperationException) t
                : new InvalidOperationException(t);
    }

    @Override
    public InvalidOperationException getCause() {
        return cause;
    }

    
}
