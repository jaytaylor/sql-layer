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

package com.akiban.server.mttests.mthapi.base;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ThreadlessRandom;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class HapiMTBase extends ApiTestBase {
    final static Logger LOG = LoggerFactory.getLogger(HapiMTBase.class);

    private static class RunThreadsException extends RuntimeException {
        private RunThreadsException(Exception cause) {
            super(cause);
        }
    }

    private final class WriteThreadCallable implements Callable<Void> {
        private final CountDownLatch setupDoneLatch = new CountDownLatch(1);
        private final WriteThread writeThread;
        private final CountDownLatch startOngoingWritesLatch;
        private boolean setupSucceeded = false;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);

        private WriteThreadCallable(WriteThread writeThread, CountDownLatch startOngoingWritesLatch) {
            ArgumentValidation.notNull("write thread", writeThread);
            ArgumentValidation.notNull("ongoing writes latch", startOngoingWritesLatch);
            this.writeThread = writeThread;
            this.startOngoingWritesLatch = startOngoingWritesLatch;
        }

        @Override
        public Void call() throws InvalidOperationException, InterruptedException {
            DDLFunctions ddl = ddl();
            DMLFunctions dml = dml();
            Session session = new SessionImpl();
            try {
                writeThread.setupWrites(ddl, dml, session);
                setupSucceeded = true;
            } finally {
                setupDoneLatch.countDown();
            }

            startOngoingWritesLatch.await();
            writeThread.ongoingWrites(ddl(), dml(), new SessionImpl(), keepGoing);
            return null;
        }

        public boolean waitForSetup() throws InterruptedException {
            setupDoneLatch.await();
            return setupSucceeded;
        }

        public void stopOngoingWrites() {
            keepGoing.set(false);
        }
    }

    private final class HapiThreadCallable implements Callable<Void> {
        private final HapiReadThread hapiReadThread;
        private final ByteArrayOutputStream outputStream;
        private final Session session = new SessionImpl();
        private final CountDownLatch startLatch;
        private final String id;

        private HapiThreadCallable(HapiReadThread hapiReadThread, String id, CountDownLatch startLatch)
        {
            this.hapiReadThread = hapiReadThread;
            outputStream = new ByteArrayOutputStream(512);
            this.startLatch = startLatch;
            this.id = id;
        }

        @Override
        public Void call() throws Exception
        {
            LOG.trace("{} call()", id);
            startLatch.await();
            LOG.trace("{} starting", id);
            final HapiGetRequest request;
            try {
                request = hapiReadThread.pullRequest(ThreadlessRandom.rand(this.hashCode()));
            } catch (RuntimeException e) {
                LOG.warn("{} failed to pull request: {}", id, e);
                throw e;
            }
            final JSONObject resultJson;
            try {
                JsonOutputter outputter = JsonOutputter.instance();
                outputStream.reset();
                hapi().processRequest(session, request, outputter, outputStream);
                String result = outputStream.toString("UTF-8");
                resultJson = new JSONObject(result);
                hapiReadThread.validateSuccessResponse(request, resultJson);
            } catch (Throwable e) {
                hapiReadThread.validateErrorResponse(request, e);
                return null;
            } finally {
                LOG.trace("{} finishing", id);
            }
            return null;
        }
    }

    protected final void runThreads(WriteThread writeThread, HapiReadThread... readThreads) {
        try {
            final CountDownLatch startAllLatch = new CountDownLatch(1);
            final ExecutorService executor = getExecutorService();

            WriteThreadCallable writeThreadCallable = new WriteThreadCallable(writeThread, startAllLatch);
            Future<Void> writeThreadFuture = executor.submit(writeThreadCallable);
            boolean setupSuccess = writeThreadCallable.waitForSetup();
            if (!setupSuccess) {
                writeThreadFuture.get();
                fail("setupSuccess was false, so we should have gotten an ExecutionException");
            }

            Collection<Callable<Void>> hapiCallables = new ArrayList<Callable<Void>>();
            for (int readThreadIndex=0; readThreadIndex < readThreads.length; ++readThreadIndex ) {
                HapiReadThread readThread = readThreads[readThreadIndex];
                final int spawnCount = readThread.spawnCount();
                if (spawnCount <= 0) {
                    throw new RuntimeException(String.format("%s: spawn count must be >0, was %s", readThread, spawnCount));
                }
                for(int spawnId=0; spawnId < spawnCount; ++spawnId) {
                    String id = String.format("HRT[%d-%d]", readThreadIndex, spawnId);
                    hapiCallables.add(new HapiThreadCallable(readThread, id, startAllLatch));
                }
            }

            List<Future<Void>> futures = new ArrayList<Future<Void>>();
            for (Callable<Void> callable : hapiCallables) {
                futures.add( executor.submit(callable) );
            }

            startAllLatch.countDown();

            List<Throwable> errors = new ArrayList<Throwable>(futures.size());
            for (Future<Void> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    errors.add(e.getCause());
                }
            }

            writeThreadCallable.stopOngoingWrites();
            writeThreadFuture.get(5, TimeUnit.SECONDS);

            if (!errors.isEmpty()) {
                List<Throwable> errorCauses = new ArrayList<Throwable>();
                int i = 1;
                int count = errors.size();
                for (Throwable error : errors) {
                    LOG.trace(String.format("Error %d of %d", i, count), error);
                    Throwable errorCause = error;
                    if (errorCause instanceof HapiReadThread.UnexpectedException) {
                        errorCause = errorCause.getCause();
                    }
                    if (errorCause instanceof ExecutionException) {
                        errorCause = errorCause.getCause();
                    }
                    errorCauses.add(errorCause);
                }
                fail("Errors: " + errorCauses);
            }
        } catch (Exception e) {
            throw new RunThreadsException(e);
        }
    }

    protected ExecutorService getExecutorService() {
        return Executors.newCachedThreadPool();
    }
}
