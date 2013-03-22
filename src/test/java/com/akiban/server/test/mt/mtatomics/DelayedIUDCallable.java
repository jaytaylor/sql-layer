/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

        public String exceptionMark(Class<? extends Exception> clazz) {
            return name() + ": " + clazz.getSimpleName();
        }
    }

    private final IUDType type;
    private final NewRow oldRow;
    private final NewRow newRow;
    private final long startDelay;
    private final long preDelay;
    private final long postDelay;

    public DelayableIUDCallable(IUDType type, NewRow oldRow, NewRow newRow,
                                long startDelay, long preDelay, long postDelay) {
        if(type == IUDType.INSERT || type == IUDType.UPDATE) {
            assertNotNull("Row to insert", newRow);
        }
        if(type == IUDType.DELETE | type == IUDType.UPDATE) {
            assertNotNull("Row to change", oldRow);
        }
        this.type = type;
        this.oldRow = oldRow;
        this.newRow = newRow;
        this.startDelay = startDelay;
        this.preDelay = preDelay;
        this.postDelay = postDelay;
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
                    dml().deleteRow(session, oldRow, false);
                break;
            }
        } catch(Exception e) {
            Timing.sleep(postDelay);
            timePoints.mark(type.exceptionMark(e.getClass()));
            throw e;
        }

        return null;
    }

    private static DMLFunctions dml() {
        return ServiceManagerImpl.get().getDXL().dmlFunctions();
    }
}
