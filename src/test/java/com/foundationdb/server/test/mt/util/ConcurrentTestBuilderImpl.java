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

import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentTestBuilderImpl implements ConcurrentTestBuilder
{
    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentTestBuilderImpl.class);

    private final Map<String,ThreadState> threadStateMap = new LinkedHashMap<>();
    private final ListMultimap<String,String> syncToThreadState = ArrayListMultimap.create();
    private String lastThreadName = null;


    public static ConcurrentTestBuilder create() {
        return new ConcurrentTestBuilderImpl();
    }

    //
    // ConcurrentTestBuilder
    //

    @Override
    public ConcurrentTestBuilder add(String name, OperatorCreator creator) {
        add(name, new ThreadState(creator));
        return this;
    }

    @Override
    public ConcurrentTestBuilder mark(ThreadMonitor.Stage... stages) {
        ThreadState state = getLastCreatorState(false);
        List<ThreadMonitor.Stage> list = Arrays.asList(stages);
        LOG.debug("mark {} at thread {}", lastThreadName, list);
        state.threadStageMarks.addAll(list);
        return this;
    }

    @Override
    public List<MonitoredThread> build(ServiceHolder serviceHolder) {
        LOG.debug("build {}", threadStateMap.keySet());
        Map<String,CyclicBarrier> barriers = new HashMap<>();
        for(Entry<String,Collection<String>> entry : syncToThreadState.asMap().entrySet()) {
            LOG.debug("barrier '{}' has parties {}", entry.getKey(), entry.getValue());
            int parties = entry.getValue().size();
            barriers.put(entry.getKey(), new CyclicBarrier(parties));
        }
        List<MonitoredThread> threads = new ArrayList<>();
        for(Entry<String, ThreadState> entry : threadStateMap.entrySet()) {
            String name = entry.getKey();
            ThreadState state = entry.getValue();
            StageMonitor monitor = new StageMonitor(barriers, state.threadStageToSyncName, state.onlineStageToSyncName);
            final MonitoredThread thread;
            if(state.creator != null) {
                thread = new MonitoredOperatorThread(name,
                                                     serviceHolder,
                                                     state.creator,
                                                     monitor,
                                                     state.threadStageMarks);
            } else {
                thread = new MonitoredDDLThread(name,
                                                serviceHolder,
                                                monitor,
                                                state.threadStageMarks,
                                                monitor,
                                                state.onlineStageMarks,
                                                state.schema,
                                                state.ddl);
            }
            threads.add(thread);
        }
        return threads;
    }

    @Override
    public ConcurrentTestBuilder sync(String name, ThreadMonitor.Stage stage) {
        LOG.debug("sync {}/{} on '{}'", new Object[] { lastThreadName, stage, name });
        ThreadState state = getLastCreatorState(false);
        String prev = state.threadStageToSyncName.put(stage, name);
        if(prev != null) {
            throw new IllegalArgumentException("Thread stage " + stage + " already latched to " + prev);
        }
        syncToThreadState.put(name, lastThreadName);
        return this;
    }

    @Override
    public ConcurrentTestBuilder add(String name, String schema, String ddl) {
        add(name, new ThreadState(schema, ddl));
        return this;
    }

    @Override
    public ConcurrentTestBuilder mark(OnlineDDLMonitor.Stage... stages) {
        ThreadState state = getLastCreatorState(true);
        List<OnlineDDLMonitor.Stage> list = Arrays.asList(stages);
        LOG.debug("mark {} at online {}", lastThreadName, list);
        state.onlineStageMarks.addAll(list);
        return this;
    }

    @Override
    public ConcurrentTestBuilder sync(String name, OnlineDDLMonitor.Stage stage) {
        LOG.debug("sync {}/{} on '{}'", new Object[] { lastThreadName, stage, name });
        ThreadState state = getLastCreatorState(false);
        String prev = state.onlineStageToSyncName.put(stage, name);
        if(prev != null) {
            throw new IllegalArgumentException("Online stage " + stage + " already latched to " + prev);
        }
        syncToThreadState.put(name, lastThreadName);
        return this;
    }

    //
    // Internal
    //

    private ConcurrentTestBuilderImpl() {
    }

    private void add(String name, ThreadState state) {
        LOG.debug("add {}", name);
        if(threadStateMap.containsKey(name)) {
            throw new IllegalArgumentException("Thread already exists: " + name);
        }
        threadStateMap.put(name, state);
        lastThreadName = name;
    }

    private ThreadState getLastCreatorState(boolean ddlRequired) {
        if(lastThreadName == null) {
            throw new IllegalStateException("No plans added");
        }
        ThreadState state = threadStateMap.get(lastThreadName);
        if(ddlRequired && (state.ddl == null)) {
            throw new IllegalStateException("Not a DDL thread");
        }
        return state;
    }

    private class ThreadState
    {
        public final OperatorCreator creator;
        private final String schema;
        private final String ddl;
        public final Set<ThreadMonitor.Stage> threadStageMarks = new HashSet<>();
        public final Map<ThreadMonitor.Stage,String> threadStageToSyncName = new HashMap<>();
        public final Set<OnlineDDLMonitor.Stage> onlineStageMarks = new HashSet<>();
        public final Map<OnlineDDLMonitor.Stage,String> onlineStageToSyncName = new HashMap<>();

        private ThreadState(OperatorCreator creator) {
            this.creator = creator;
            this.schema = this.ddl = null;
        }

        private ThreadState(String schema, String ddl) {
            this.creator = null;
            this.schema = schema;
            this.ddl = ddl;
        }
    }

    private static class StageMonitor implements ThreadMonitor, OnlineDDLMonitor
    {
        private final Map<String,CyclicBarrier> barriers;
        private final Map<ThreadMonitor.Stage,String> threadStageToBarrier;
        private final Map<OnlineDDLMonitor.Stage,String> onlineStageToBarrier;

        private StageMonitor(Map<String, CyclicBarrier> barriers,
                             Map<ThreadMonitor.Stage, String> threadStageToBarrier,
                             Map<OnlineDDLMonitor.Stage, String> onlineStageToBarrier) {
            this.barriers = barriers;
            this.threadStageToBarrier = threadStageToBarrier;
            this.onlineStageToBarrier = onlineStageToBarrier;
        }

        @Override
        public void at(ThreadMonitor.Stage stage) throws InterruptedException, BrokenBarrierException {
            String barrierName = threadStageToBarrier.get(stage);
            atBarrier(barrierName);
        }

        @Override
        public void at(OnlineDDLMonitor.Stage stage) {
            String barrierName = onlineStageToBarrier.get(stage);
            try {
                atBarrier(barrierName);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void atBarrier(String barrierName) throws BrokenBarrierException, InterruptedException {
            if(barrierName != null) {
                CyclicBarrier barrier = barriers.get(barrierName);
                barrier.await();
            }
        }
    }
}
