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

package com.akiban.qp.loadableplan.std;

import java.rmi.RemoteException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.operator.BindingNotSetException;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.service.session.Session;
import com.persistit.Management.TaskStatus;
import com.persistit.Persistit;
import com.persistit.Task;

/**
 * Invokes the akiban-persistit CLI via a PSQL statement through a loadable
 * plan.
 */
public class PersistitCLILoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context) {
                return new PersistitCliDirectObjectCursor(context);
            }

            @Override
            public OutputMode getOutputMode() {
                return OutputMode.COPY_WITH_NEWLINE;
            }
        };
    }

    public static class PersistitCliDirectObjectCursor extends DirectObjectCursor {
        final QueryContext context;
        final Persistit db;
        final Session session;
        boolean done = false;
        boolean delivered = false;
        long taskId;
        ArrayList<String> messages = new ArrayList<String>();

        public PersistitCliDirectObjectCursor(QueryContext context) {
            this.context = context;
            this.db = ((PersistitAdapter)context.getStore()).persistit().getDb();
            this.session = context.getSession();
        }

        @Override
        public void open() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                String carg;
                try {
                    carg = context.getValue(i).getString();
                } catch (BindingNotSetException ex) {
                    break;
                }
                sb.append(carg);
                sb.append(' ');
            }
            final String taskIdString;
            try {
                taskIdString = db.getManagement().launch(sb.toString());
            } catch (RemoteException e) {
                throw new AkibanInternalException(e.toString());
            }
            try {
                taskId = Long.parseLong(taskIdString);
            } catch (NumberFormatException e) {
                messages.add("Not launched: " + taskIdString);
                done = true;
            }
        }

        @Override
        public List<String> next() {

            if (messages.isEmpty()) {
                try {
                    if (done) {
                        return null;
                    }
                    TaskStatus[] tsArray = db.getManagement().queryTaskStatus(taskId, true, true);
                    if (tsArray.length != 1) {
                        messages.add("Invalid queryTask response: " + tsArray);
                        stopTask();
                    }
                    TaskStatus ts = tsArray[0];
                    for (final String message : ts.getMessages()) {
                        messages.add(message);
                    }
                    switch (ts.getState()) {
                    case Task.STATE_DONE:
                        done = true;
                        break;
                    case Task.STATE_ENDED:
                        done = true;
                        messages.add("Task " + taskId + " ended");
                        break;
                    case Task.STATE_EXPIRED:
                        done = true;
                        messages.add("Task " + taskId + " time limit expired");
                        break;
                    case Task.STATE_FAILED:
                        done = true;
                        messages.add("Task " + taskId + " failed");
                        break;
                    default:
                        // continue
                    }
                    if (!done && messages.isEmpty()) {
                        Thread.sleep(TIMEOUT);
                    }

                } catch (InterruptedException ex) {
                    stopTask();
                    throw new QueryCanceledException(session);
                } catch (Exception e) {
                    messages.add(e.toString());
                    stopTask();
                    done = true;
                }
            }
            if (messages.isEmpty()) {
                return messages;
            } else {
                String message= messages.remove(0);
                return Collections.singletonList(message);
            }
        }

        @Override
        public void close() {
            stopTask();
        }

        private void stopTask() {
            try {
                db.getManagement().stopTask(taskId, true);
            } catch (RemoteException e) {
                throw new AkibanInternalException(e.toString());
            }
        }
    }

    @Override
    public int[] jdbcTypes() {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
    private static final long TIMEOUT = 500;
}
