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

import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.test.mt.mtutil.TimePoints;
import com.akiban.server.test.mt.mtutil.TimedCallable;
import com.akiban.server.test.mt.mtutil.Timing;
import com.akiban.server.service.dxl.ConcurrencyAtomicsDXLService;
import com.akiban.server.service.session.Session;

import static org.junit.Assert.assertNotNull;

class DelayableIUDCallable extends TimedCallable<Void> {
    public enum IUDType {
        INSERT,
        UPDATE,
        DELETE
        ;

        public String startMark() {
            return name() + ": START";
        }

        public String inMark() {
            return name() + ">";
        }

        public String outMark() {
            return name() + "<";
        }

        public String finishMark() {
            return name() + ": START";
        }
    }

    private final IUDType type;
    private final NewRow oldRow;
    private final NewRow newRow;
    private final long startDelay;
    private final long preDelay;
    private final long postDelay;
    private final long finishDelay;

    public DelayableIUDCallable(IUDType type, NewRow oldRow, NewRow newRow,
                                long startDelay, long preDelay, long postDelay, long finishDelay) {
        if(type == IUDType.INSERT || type == IUDType.UPDATE) {
            assertNotNull("Row to insert", newRow);
        }
        if(type == IUDType.DELETE | type == IUDType.UPDATE) {
            assertNotNull("Row to change", newRow);
        }
        this.type = type;
        this.oldRow = oldRow;
        this.newRow = newRow;
        this.startDelay = startDelay;
        this.preDelay = preDelay;
        this.postDelay = postDelay;
        this.finishDelay = finishDelay;
    }

    @Override
    protected final Void doCall(final TimePoints timePoints, Session session) throws Exception {
        ConcurrencyAtomicsDXLService.hookNextIUD(
                session,
                new Runnable() {
                    @Override
                    public void run() {
                        timePoints.mark(type.inMark());
                        Timing.sleep(preDelay);
                    }
                },
                new Runnable() {
                    @Override
                    public void run() {
                        Timing.sleep(postDelay);
                        timePoints.mark(type.outMark());
                    }
                }
        );

        Timing.sleep(startDelay);
        timePoints.mark(type.startMark());
        try {
            switch(type) {
                case INSERT:
                    dml().writeRow(session, newRow);
                break;
                case UPDATE:
                    dml().updateRow(session, oldRow, newRow, null);
                break;
                case DELETE:
                    dml().deleteRow(session, oldRow);
                break;
            }
        } catch(Exception e) {
            timePoints.mark(type.name() + ": exception " + e.getClass().getSimpleName());
            throw e;
        }

        Timing.sleep(finishDelay);
        timePoints.mark(type.finishMark());
        return null;
    }

    private static DMLFunctions dml() {
        return ServiceManagerImpl.get().getDXL().dmlFunctions();
    }
}
