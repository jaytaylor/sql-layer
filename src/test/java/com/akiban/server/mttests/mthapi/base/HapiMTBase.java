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

import com.akiban.ais.model.Index;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mthapi.common.HapiValidationError;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.ThreadlessRandom;
import com.akiban.util.WeightedRandom;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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

    private final class WriteThreadCallable implements Callable<WriteThreadStats> {
        private final CountDownLatch setupDoneLatch = new CountDownLatch(1);
        private final WriteThread writeThread;
        private boolean setupSucceeded = false;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);
        private final Map<EqualishExceptionWrapper,Integer> errors;

        private WriteThreadCallable(WriteThread writeThread, Map<EqualishExceptionWrapper,Integer> errors) {
            ArgumentValidation.notNull("write thread", writeThread);
            this.writeThread = writeThread;
            this.errors = errors;
        }

        @Override
        public WriteThreadStats call() throws InvalidOperationException, InterruptedException {
            DDLFunctions ddl = ddl();
            DMLFunctions dml = dml();
            Session session = new SessionImpl();
            try {
                writeThread.setupWrites(ddl, dml, session);
                setupSucceeded = true;
            } finally {
                setupDoneLatch.countDown();
            }

            boolean exceptionsNotFatal = true;
            while (exceptionsNotFatal && keepGoing.get()) {
                try {
                    writeThread.ongoingWrites(ddl(), dml(), new SessionImpl(), keepGoing);
                } catch (Throwable t) {
                    addError(t, errors);
                    exceptionsNotFatal = writeThread.continueThroughException(t);
                }
            }
            return writeThread.getStats();
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
        private final String id;

        private HapiThreadCallable(HapiReadThread hapiReadThread, String id)
        {
            this.hapiReadThread = hapiReadThread;
            outputStream = new ByteArrayOutputStream(512);
            this.id = id;
        }

        @Override
        public Void call() throws Exception
        {
            LOG.trace("{} starting", id);
            final HapiRequestStruct requestStruct;
            final HapiGetRequest request;
            try {
                requestStruct = hapiReadThread.pullRequest(ThreadlessRandom.rand(this.hashCode()));
            } catch (RuntimeException e) {
                LOG.warn("{} failed to pull request: {}", id, e);
                throw e;
            }
            final JSONObject resultJson;
            request = requestStruct.getRequest();
            try {
                JsonOutputter outputter = JsonOutputter.instance();
                outputStream.reset();
                Index index = hapi().findHapiRequestIndex(session, request);
                hapiReadThread.validateIndex(requestStruct, index);
                hapi().processRequest(session, request, outputter, outputStream);
                String result = outputStream.toString("UTF-8");
                resultJson = new JSONObject(result);
            } catch (Throwable e) {
                LOG.trace("{} errored", id);
                hapiReadThread.validateErrorResponse(request, e);
                return null;
            }
            try {
                hapiReadThread.validateSuccessResponse(requestStruct, resultJson);
            } catch (HapiValidationError e) {
                e.setJsonObject(resultJson);
                e.setGetRequest(request);
                throw e;
            }
            LOG.trace("{} verified", id);
            return null;
        }
    }

    protected final void runThreads(WriteThread writeThread, HapiReadThread... readThreads) {
        try {
            final ExecutorService executor = Executors.newFixedThreadPool( executorsCount() );
            Map<EqualishExceptionWrapper,Integer> errors = new ConcurrentHashMap<EqualishExceptionWrapper, Integer>();

            WriteThreadCallable writeThreadCallable = new WriteThreadCallable(writeThread, errors);
            Future<WriteThreadStats> writeThreadFuture = executor.submit(writeThreadCallable);
            boolean setupSuccess = writeThreadCallable.waitForSetup();
            if (!setupSuccess) {
                writeThreadFuture.get();
                fail("setupSuccess was false, so we should have gotten an ExecutionException");
            }

            Map<EqualishExceptionWrapper,Integer> errorsMap = feedReadThreads(readThreads, executor, errors);

            writeThreadCallable.stopOngoingWrites();
            WriteThreadStats stats = writeThreadFuture.get(5, TimeUnit.SECONDS);

            LOG.trace("{} writes", stats.getWrites());

            if (!errorsMap.isEmpty()) {
                failWithErrors(errorsMap);
            }
        } catch (Exception e) {
            throw new RunThreadsException(e);
        }
    }

    private Map<EqualishExceptionWrapper,Integer> feedReadThreads(HapiReadThread[] readThreads,
                                                                  ExecutorService executorService,
                                                                  Map<EqualishExceptionWrapper, Integer> errors)
            throws InterruptedException
    {
        final ExecutorService processingService = Executors.newFixedThreadPool( processersCount() );

        ArrayBlockingQueue<Future<Void>> submitFutures = new ArrayBlockingQueue<Future<Void>>(
                maxPendingReadsCount()
        );

        // Set up the processing threads
        List<Future<Void>> processingFutures = new ArrayList<Future<Void>>();
        List<ReadThreadProcessor> processors = new ArrayList<ReadThreadProcessor>();
        for (int i = 0, MAX = processersCount(); i < MAX; ++i) {
            ReadThreadProcessor processor = new ReadThreadProcessor(submitFutures, errors);
            processors.add(processor);
            Future<Void> processingFuture = processingService.submit(processor);
            processingFutures.add(processingFuture);
        }

        // Feed HapiReadThreads into the executorService as fast as the processing threads can process the results
        WeightedRandom<HapiReadThread> randomThreads = new WeightedRandom<HapiReadThread>(readThreads);
        for (HapiReadThread readThread : readThreads) {
            randomThreads.setWeight(readThread, readThread.spawnCount());
        }
        while (randomThreads.hasWeights()) {
            HapiReadThread hapiReadThread = randomThreads.get(-1);
            HapiThreadCallable callable = new HapiThreadCallable(hapiReadThread, "yo");
            Future<Void> submitFuture = executorService.submit(callable);
            submitFutures.put(submitFuture);
        }

        // Tell the processors that we're done, and wait for them to finish
        for (ReadThreadProcessor processor : processors) {
            processor.stopProcessing();
        }
        for (Future<Void> processingFuture : processingFutures) {
            try {
                processingFuture.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return errors;
    }

    protected int processersCount() {
        return executorsCount() * 2;
    }

    protected int executorsCount() {
        return 16;
    }

    protected int maxPendingReadsCount() {
        return 100;
    }

    private static void failWithErrors(Map<EqualishExceptionWrapper,Integer> errors) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printer = new PrintWriter(stringWriter);

        int pairsCount = errors.size();
        printer.format("%d failure pattern", pairsCount);
        if (pairsCount != 1) {
            printer.print('s');
        }
        printer.println(':');
        for (Map.Entry<EqualishExceptionWrapper,Integer> pair : errors.entrySet()) {
            Throwable error = pair.getKey().get();
            int count = pair.getValue();

            printer.format("%d instance", count);
            if (count != 1) {
                printer.print('s');
            }
            printer.format(" of this general pattern:\n");

            error.printStackTrace(printer);
        }
        printer.flush();
        stringWriter.flush();
        fail(stringWriter.toString());
    }

    private static class ReadThreadProcessor implements Callable<Void> {
        private final ArrayBlockingQueue<Future<Void>> futures;
        private final Map<EqualishExceptionWrapper,Integer> errorsMap;
        private volatile boolean active = true;

        private ReadThreadProcessor(ArrayBlockingQueue<Future<Void>> futures,
                                    Map<EqualishExceptionWrapper, Integer> errorsMap
        ) {
            this.futures = futures;
            this.errorsMap = errorsMap;
        }

        @Override
        public Void call() throws InterruptedException {
            while(active) {
                Future<Void> future = futures.take();
                try {
                    future.get();
                } catch (ExecutionException e) {
                    addError(e.getCause(), errorsMap);
                }
            }
            return null;
        }

        public void stopProcessing() {
            active = false;
        }
    }

    private static void addError(Throwable error, Map<EqualishExceptionWrapper,Integer> errorsMap)  {
        EqualishExceptionWrapper wrapper = new EqualishExceptionWrapper(error);
        synchronized (errorsMap) {
            Integer count = errorsMap.get(wrapper);
            count = (count == null) ? 1 : count + 1;
            errorsMap.put(wrapper, count);
        }
    }
}
