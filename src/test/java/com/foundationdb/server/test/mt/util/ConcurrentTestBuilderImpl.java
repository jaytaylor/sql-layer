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

    private final Map<String,ThreadState> creatorMap = new LinkedHashMap<>();
    private final ListMultimap<String,String> syncToCreatorMap = ArrayListMultimap.create();
    private String lastCreatorName = null;


    public static ConcurrentTestBuilder create() {
        return new ConcurrentTestBuilderImpl();
    }

    //
    // ConcurrentTestBuilder
    //

    @Override
    public ConcurrentTestBuilder add(String name, OperatorCreator creator) {
        LOG.debug("add {}", name);
        if(creatorMap.containsKey(name)) {
            throw new IllegalArgumentException("Plan already exists: " + name);
        }
        creatorMap.put(name, new ThreadState(creator));
        lastCreatorName = name;
        return this;
    }

    @Override
    public ConcurrentTestBuilder mark(ThreadMonitor.Stage... stages) {
        ThreadState state = getLastCreatorState();
        List<ThreadMonitor.Stage> list = Arrays.asList(stages);
        LOG.debug("mark {} at {}", lastCreatorName, list);
        state.stageMarks.addAll(list);
        return this;
    }

    @Override
    public List<MonitoredOperatorThread> build(ServiceHolder serviceHolder) {
        LOG.debug("build {}", creatorMap.keySet());
        Map<String,CyclicBarrier> barriers = new HashMap<>();
        for(Entry<String,Collection<String>> entry : syncToCreatorMap.asMap().entrySet()) {
            LOG.debug("barrier '{}' has parties {}", entry.getKey(), entry.getValue());
            int parties = entry.getValue().size();
            barriers.put(entry.getKey(), new CyclicBarrier(parties));
        }
        List<MonitoredOperatorThread> threads = new ArrayList<>();
        for(Entry<String, ThreadState> entry : creatorMap.entrySet()) {
            String name = entry.getKey();
            ThreadState state = entry.getValue();
            StageMonitor monitor = new StageMonitor(barriers, state.stageToSyncName);
            threads.add(new MonitoredOperatorThread(name, serviceHolder, state.creator, monitor, state.stageMarks));
        }
        return threads;
    }

    @Override
    public ConcurrentTestBuilder sync(String name, ThreadMonitor.Stage stage) {
        LOG.debug("sync {}/{} on '{}'", new Object[] { lastCreatorName, stage, name });
        ThreadState state = getLastCreatorState();
        String prev = state.stageToSyncName.put(stage, name);
        if(prev != null) {
            throw new IllegalArgumentException("Stage " + stage + " already latched to " + prev);
        }
        syncToCreatorMap.put(name, lastCreatorName);
        return this;
    }

    //
    // Internal
    //

    private ConcurrentTestBuilderImpl() {
    }

    private ThreadState getLastCreatorState() {
        if(lastCreatorName == null) {
            throw new IllegalStateException("No plans added");
        }
        return creatorMap.get(lastCreatorName);
    }

    private static class ThreadState
    {
        public final OperatorCreator creator;
        public final Set<ThreadMonitor.Stage> stageMarks;
        public final Map<ThreadMonitor.Stage,String> stageToSyncName;

        private ThreadState(OperatorCreator creator) {
            this.creator = creator;
            this.stageMarks = new HashSet<>();
            this.stageToSyncName = new HashMap<>();
        }
    }

    private static class StageMonitor implements ThreadMonitor
    {
        private final Map<String,CyclicBarrier> barriers;
        private final Map<Stage,String> stageToBarrier;

        private StageMonitor(Map<String, CyclicBarrier> barriers, Map<Stage, String> stageToBarrier) {
            this.barriers = barriers;
            this.stageToBarrier = stageToBarrier;
        }

        @Override
        public void at(Stage stage) throws InterruptedException, BrokenBarrierException {
            String barrierName = stageToBarrier.get(stage);
            if(barrierName != null) {
                CyclicBarrier barrier = barriers.get(barrierName);
                barrier.await();
            }
        }
    }
}
