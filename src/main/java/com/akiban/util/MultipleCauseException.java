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

package com.akiban.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class MultipleCauseException extends RuntimeException {
    private final List<Throwable> causes = new ArrayList<Throwable>();

    public void addCause(Throwable cause) {
        causes.add(cause);
    }

    @Override
    public String toString() {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        printWriter.printf("%d cause%s%n", causes.size(), causes.size() == 1 ? ":" : "s:");
        for (Enumerated<Throwable> cause : EnumeratingIterator.of(causes)) {
            printWriter.printf("%d:%n----------------------------%n", cause.count());
            cause.get().printStackTrace(printWriter);
        }
        printWriter.flush();
        stringWriter.flush();
        return stringWriter.toString();
    }

    public List<Throwable> getCauses() {
        return Collections.unmodifiableList(causes);
    }
    
    public static RuntimeException combine(RuntimeException oldProblem, RuntimeException newProblem) {
        if (oldProblem == null)
            return newProblem;
        if (oldProblem instanceof MultipleCauseException) {
            MultipleCauseException mce = (MultipleCauseException) oldProblem;
            mce.addCause(newProblem);
            return mce;
        }
        MultipleCauseException mce = new MultipleCauseException();
        mce.addCause(oldProblem);
        mce.addCause(newProblem);
        return mce;
    }
}
