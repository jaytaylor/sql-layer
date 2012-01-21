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

package com.akiban.server.service.tree;

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
    public String name() {
        return "persistitcli";
    }

    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context) {
                return new PersistitCliDirectObjectCursor(context);
            }

            @Override
            public boolean useCopyData() {
                return true;
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
