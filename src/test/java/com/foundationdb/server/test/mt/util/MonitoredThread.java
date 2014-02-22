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

package com.foundationdb.server.test.mt.util;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.TableVersionChangedException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public abstract class MonitoredThread extends Thread implements HasTimeMarker
{
    protected static final Logger LOG = LoggerFactory.getLogger(MonitoredThread.class);

    private final ServiceHolder services;
    private final ThreadMonitor monitor;
    private final Collection<Stage> stageMarks;

    private final Collection<Stage> completedStages;
    private final TimeMarker timeMarker;
    private final List<Row> scannedRows;
    private boolean hadRollback;
    private Throwable failure;

    protected MonitoredThread(String name,
                              ServiceHolder services,
                              ThreadMonitor monitor,
                              Collection<Stage> stageMarks) {
        super(name);
        this.services = services;
        this.monitor = monitor;
        this.stageMarks = stageMarks;
        this.completedStages = new HashSet<>();
        this.timeMarker = new TimeMarker();
        this.scannedRows = new ArrayList<>();
    }

    //
    // MonitoredThread
    //

    protected abstract boolean doRetryOnRollback();
    protected abstract boolean doRetryOnTableVersionChange();

    public final boolean hadRollback() {
        return hadRollback;
    }

    public final boolean hadFailure() {
        return (failure != null);
    }

    public final Throwable getFailure() {
        return failure;
    }

    public final List<Row> getScannedRows() {
        return scannedRows;
    }

    protected final ServiceHolder getServiceHolder() {
        return services;
    }

    protected void mark(String msg) {
        timeMarker.mark(getName() + ":" + msg);
    }

    protected void to(Stage stage) {
        try {
            boolean firstTime = !completedStages.contains(stage);
            if(firstTime) {
                monitor.at(stage);
            }
            completedStages.add(stage);
            LOG.trace("to: {}", stage);
            if(firstTime && stageMarks.contains(stage)) {
                mark(stage.name());
            }
        } catch(Throwable t) {
            wrap(t);
        }
    }

    protected abstract void runInternal(Session session) throws Exception;

    //
    // HasTimeMarker
    //

    @Override
    public final TimeMarker getTimeMarker() {
        return timeMarker;
    }

    //
    // Thread
    //

    @Override
    public final void run() {
        to(ThreadMonitor.Stage.START);
        try(Session session = services.createSession()) {
            for(;;) {
                try {
                    runInternal(session);
                    break;
                } catch(InvalidOperationException e) {
                    hadRollback |= e.getCode().isRollbackClass();
                    if((e instanceof TableVersionChangedException) && !doRetryOnTableVersionChange()) {
                        throw e;
                    } else if(!e.getCode().isRollbackClass() || !doRetryOnRollback()) {
                        throw e;
                    }
                    LOG.debug("retrying due to", e);
                    getScannedRows().clear();
                }
            }
        } catch(Throwable t) {
            wrap(t);
        } finally {
            to(Stage.FINISH);
        }
    }

    //
    // Internal
    //

    private void wrap(Throwable t) {
        LOG.debug("wrapping", t);
        if(failure == null) {
            failure = t;
        }
        mark(t.getClass().getSimpleName());
        if(t instanceof Error) {
            throw (Error)t;
        }
        if(t instanceof RuntimeException) {
            throw (RuntimeException)t;
        }
        throw new RuntimeException(t);
    }
}
