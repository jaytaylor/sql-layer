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

package com.akiban.server.mttests.mthapi.base;

import java.util.Arrays;

public final class EqualishExceptionWrapper {
    private final Throwable throwable;

    public EqualishExceptionWrapper(Throwable throwable) {
        this.throwable = throwable;
    }

    public Throwable get() {
        return throwable;
    }

    @Override
    public int hashCode() {
        int result = 0;
        Throwable myThrowable = throwable;
        while (myThrowable != null) {
            result = 31 * result + (myThrowable.getClass().hashCode());
            result = 31 * result + (Arrays.hashCode(myThrowable.getStackTrace()));
            myThrowable = myThrowable.getCause();
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof EqualishExceptionWrapper) ) {
            return false;
        }
        EqualishExceptionWrapper other = (EqualishExceptionWrapper) obj;

        Throwable myThrowable = throwable;
        Throwable otherThrowable = other.throwable;

        while (myThrowable != null) {
            if (otherThrowable == null) {
                return false;
            }
            if (!myThrowable.getClass().equals(otherThrowable.getClass())) {
                return false;
            }
            StackTraceElement[] myTrace = myThrowable.getStackTrace();
            StackTraceElement[] otherTrace = otherThrowable.getStackTrace();
            if (!Arrays.equals(myTrace, otherTrace)) {
                return false;
            }
            myThrowable = myThrowable.getCause();
            otherThrowable = otherThrowable.getCause();
        }

        return otherThrowable == null; // otherwise, the other guy had an extra cause
    }
}
