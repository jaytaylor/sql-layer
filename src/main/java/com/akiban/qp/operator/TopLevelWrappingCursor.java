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

package com.akiban.qp.operator;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

class TopLevelWrappingCursor extends ChainedCursor {

    // Cursor interface

    @Override
    public void open() {
        try {
            CURSOR_SETUP_TAP.in();
            try {
                super.open();
            } finally {
                CURSOR_SETUP_TAP.out();
            }
            CURSOR_SCAN_TAP.in();
            closed = false;
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    @Override
    public void close() {
        try {
            // CURSOR_SCAN_TAP.in() was done in the open call. The operator implementation is supposed to guarantee
            // at least one close per open, so we'll have to rely on that. But multiple closes per open are permitted,
            // so be careful to leave the tap no more than once.
            if (!closed) {
                super.close();
                CURSOR_SCAN_TAP.out();
                closed = true;
            }
        } catch (RuntimeException e) {
            throw launder(e);
        }
    }

    // WrappingCursor interface

    TopLevelWrappingCursor(QueryContext context, Cursor input) {
        super(context, input);
    }

    // private methods

    private static RuntimeException launder(RuntimeException exception) {
        return exception;
    }

    // Class state

    private static final InOutTap CURSOR_SETUP_TAP = Tap.createTimer("cursor setup");
    private static final InOutTap CURSOR_SCAN_TAP = Tap.createTimer("cursor scan");

    // Object state

    private boolean closed;

}
