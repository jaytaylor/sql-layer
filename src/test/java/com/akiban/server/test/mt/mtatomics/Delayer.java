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

package com.akiban.server.test.mt.mtatomics;

import com.akiban.server.test.mt.mtutil.TimePoints;
import com.akiban.server.test.mt.mtutil.Timing;
import com.akiban.util.ArgumentValidation;

class Delayer {
    private final long[] delays;
    private final String[] messagesBefore;
    private final String[] messagesAfter;
    private final TimePoints timePoints;
    private int count;

    Delayer(TimePoints timePoints, long... delays) {
        this.delays = new long[delays.length];
        this.messagesBefore = timePoints == null ? null : new String[delays.length];
        this.messagesAfter = timePoints == null ? null : new String[delays.length];
        System.arraycopy(delays, 0, this.delays, 0, delays.length);
        this.timePoints = timePoints;
    }

    public void delay() {
        if (count >= delays.length) {
            ++count; // not useful, just for record keeping (in case we look at this field in a debugger)
            return;
        }
        long delay = count >= delays.length ? -1 : delays[count];
        mark(messagesBefore);
        Timing.sleep(delay);
        mark(messagesAfter);
        ++count;
    }

    private void mark(String[] messages) {
        if (timePoints != null) {
            String message = messages[count];
            if (message != null) {
                timePoints.mark(message);
            }
        }
    }

    public Delayer markBefore(int index, String text) {
        defineMessage(index, text, messagesBefore);
        return this;
    }

    public Delayer markAfter(int index, String text) {
        defineMessage(index, text, messagesAfter);
        return this;
    }

    private void defineMessage(int index, String text, String[] messages) {
        ArgumentValidation.isGTE("index", index, 0);
        ArgumentValidation.isLT("index", index, delays.length);
        ArgumentValidation.notNull("timepoints", messages);
        messages[index] = text;
    }
}
