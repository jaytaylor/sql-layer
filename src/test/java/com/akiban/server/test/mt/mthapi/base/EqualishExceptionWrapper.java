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

package com.akiban.server.test.mt.mthapi.base;

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
