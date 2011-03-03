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

package com.akiban.server.mttests.mthapi;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.util.ArgumentValidation;
import org.json.JSONObject;

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

import static org.junit.Assert.*;

public class HapiMTBase extends ApiTestBase {

    private final class WriteThreadCallable implements Callable<Void> {
        private final CountDownLatch setupDoneLatch = new CountDownLatch(1);
        private final WriteThread writeThread;
        private final CountDownLatch startOngoingWritesLatch;
        private boolean setupSucceeded = false;

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
            writeThread.ongoingWrites(ddl(), dml(), new SessionImpl());
            return null;
        }

        public boolean waitForSetup() throws InterruptedException {
            setupDoneLatch.await();
            return setupSucceeded;
        }
    }

    private final class HapiThreadCallable implements Callable<Void> {
        private final HapiReadThread hapiReadThread;
        private final ByteArrayOutputStream outputStream;
        private final Session session = new SessionImpl();
        private final CountDownLatch startLatch;

        private HapiThreadCallable(HapiReadThread hapiReadThread, CountDownLatch startLatch) {
            this.hapiReadThread = hapiReadThread;
            outputStream = new ByteArrayOutputStream(512);
            this.startLatch = startLatch;
        }

        @Override
        public Void call()
                throws HapiReadThread.UnexpectedException, HapiReadThread.UnexpectedSuccess, InterruptedException
        {
            startLatch.await();
            final HapiGetRequest request = hapiReadThread.pullRequest();
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
            }
            hapiReadThread.validateSuccessResponse(request, resultJson);
            return null;
        }
    }

    protected final void runThreads(WriteThread writeThread, HapiReadThread... readThreads)
            throws InterruptedException, ExecutionException
    {
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
        for (HapiReadThread readThread : readThreads) {
            int spawnCount = readThread.spawnCount();
            if (spawnCount <= 0) {
                throw new RuntimeException(String.format("%s: spawn count must be >0, was %s", readThread, spawnCount));
            }
            while (spawnCount-- > 0) {
                hapiCallables.add(new HapiThreadCallable(readThread, startAllLatch));
            }
        }

        List<Future<Void>> futures = executor.invokeAll(hapiCallables);
        List<Throwable> errors = new ArrayList<Throwable>(futures.size());
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                errors.add(e.getCause());
            }
        }

        if (!errors.isEmpty()) {
            fail("Errors: " + errors);
        }
    }

    protected ExecutorService getExecutorService() {
        return Executors.newCachedThreadPool();
    }
}
