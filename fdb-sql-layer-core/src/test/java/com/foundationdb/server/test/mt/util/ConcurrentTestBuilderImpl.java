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
import com.foundationdb.server.test.mt.OnlineCreateTableAsMT;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.util.RandomRule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import org.junit.ClassRule;
import org.junit.Rule;
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
//import java.util.Random;
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
    public List<MonitoredThread> build(ServiceHolder serviceHolder){
        return build(serviceHolder, null, null, null);
    }
    @Override
    public List<MonitoredThread> build(ServiceHolder serviceHolder, List<DataTypeDescriptor> descriptors,
                                       List<String> columnNames, OnlineCreateTableAsBase.TestSession server) {
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
                                                     state.threadStageMarks,
                                                     state.retryOnRollback);
            } else {
                thread = new MonitoredDDLThread(name,
                                                serviceHolder,
                                                monitor,
                                                state.threadStageMarks,
                                                monitor,
                                                state.onlineStageMarks,
                                                state.schema,
                                                state.ddl,
                                                descriptors,
                                                columnNames,
                                                server);
            }
            threads.add(thread);
        }
        return threads;
    }

    @Override
    public ConcurrentTestBuilder sync(String name, ThreadMonitor.Stage stage) {
        return sync(lastThreadName, name, stage);
    }

    @Override
    public ConcurrentTestBuilder rollbackRetry(boolean doRetry) {
        ThreadState state = getThreadState(lastThreadName, false);
        state.retryOnRollback = doRetry;
        return this;
    }

    @Override
    public ConcurrentTestBuilder sync(String testName, String syncName, Stage stage) {
        LOG.debug("sync {}/{} on '{}'", new Object[] { testName, stage, syncName });
        ThreadState state = getThreadState(testName, false);
        String prev = state.threadStageToSyncName.put(stage, syncName);
        if(prev != null) {
            throw new IllegalArgumentException("Thread stage " + stage + " already latched to " + prev);
        }
        syncToThreadState.put(syncName, testName);
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
        return sync(lastThreadName, name, stage);
    }

    @Override
    public ConcurrentTestBuilder sync(String testName, String syncName, OnlineDDLMonitor.Stage stage) {
        LOG.debug("sync {}/{} on '{}'", new Object[] { testName, stage, syncName });
        ThreadState state = getThreadState(testName, false);
        String prev = state.onlineStageToSyncName.put(stage, syncName);
        if(prev != null) {
            throw new IllegalArgumentException("Online stage " + stage + " already latched to " + prev);
        }
        syncToThreadState.put(syncName, testName);
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
        return getThreadState(lastThreadName, ddlRequired);
    }

    private ThreadState getThreadState(String name, boolean ddlRequired) {
        ThreadState state = threadStateMap.get(name);
        if(state == null) {
            throw new IllegalArgumentException("Unknown thread name: " + name);
        }
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
        public boolean retryOnRollback = true;

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
            delay(true, stage == DELAY_THREAD_STAGE);
            String barrierName = threadStageToBarrier.get(stage);
            atBarrier(barrierName);
            delay(false, stage == DELAY_THREAD_STAGE);
        }

        @Override
        public void at(OnlineDDLMonitor.Stage stage) {
            delay(true, stage == DELAY_DDL_STAGE);
            String barrierName = onlineStageToBarrier.get(stage);
            try {
                atBarrier(barrierName);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
            delay(false, stage == DELAY_DDL_STAGE);
        }

        private void atBarrier(String barrierName) throws BrokenBarrierException, InterruptedException {
            if(barrierName != null) {
                CyclicBarrier barrier = barriers.get(barrierName);
                barrier.await();
            }
        }

        private void delay(boolean isBefore, boolean stageMatched) {
            if((isBefore == DELAY_BEFORE) && stageMatched) {
                try {
                    Thread.sleep(50);
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @ClassRule
    public static final RandomRule randomRule = new RandomRule();
    @Rule
    public final RandomRule delayRule = randomRule;
    
    @SafeVarargs
    private static <T> T choose(T... values) {
        return values[randomRule.getRandom().nextInt(values.length)];
    }
    public static final boolean DELAY_BEFORE = randomRule.getRandom().nextBoolean();
    public static final OnlineDDLMonitor.Stage DELAY_DDL_STAGE = choose(OnlineDDLMonitor.Stage.values());
    public static final ThreadMonitor.Stage DELAY_THREAD_STAGE = choose(ThreadMonitor.Stage.values());
}
