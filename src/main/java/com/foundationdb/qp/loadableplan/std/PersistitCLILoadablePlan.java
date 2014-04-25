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

package com.foundationdb.qp.loadableplan.std;

import java.rmi.RemoteException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.service.session.Session;
import com.persistit.Management.TaskStatus;
import com.persistit.Persistit;
import com.persistit.Task;

/**
 * Invokes the Persistit CLI via a PSQL statement through a loadable
 * plan.
 */
public class PersistitCLILoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                return new PersistitCliDirectObjectCursor(context, bindings);
            }

            @Override
            public OutputMode getOutputMode() {
                return OutputMode.COPY_WITH_NEWLINE;
            }
        };
    }

    public static class PersistitCliDirectObjectCursor extends DirectObjectCursor {
        final QueryContext context;
        final QueryBindings bindings;
        final Persistit db;
        final Session session;
        boolean done = false;
        boolean delivered = false;
        long taskId;
        ArrayList<String> messages = new ArrayList<>();

        public PersistitCliDirectObjectCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
            this.db = ((PersistitAdapter)context.getStore()).persistit().getDb();
            this.session = context.getSession();
        }

        @Override
        public void open() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                String carg;
                try {
                    carg = bindings.getValue(i).getString();
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
