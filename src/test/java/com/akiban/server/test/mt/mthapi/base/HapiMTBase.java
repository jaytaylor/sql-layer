/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.mt.mthapi.base;

import com.akiban.ais.model.Index;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.test.mt.MTBase;
import com.akiban.server.test.mt.mthapi.common.HapiValidationError;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
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
import java.util.Collections;
import java.util.Comparator;
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

public class HapiMTBase extends MTBase {
    final static Logger LOG = LoggerFactory.getLogger(HapiMTBase.class);

    private static class RunThreadsException extends RuntimeException {
        private RunThreadsException(Exception cause) {
            super(cause);
        }
    }

    private final class WriteThreadCallable implements Callable<Void> {
        private final CountDownLatch setupDoneLatch = new CountDownLatch(1);
        private final WriteThread writeThread;
        private boolean setupSucceeded = false;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);
        private final Map<EqualishExceptionWrapper,Integer> errors;
        private final Runnable fatalExceptionCallback;

        private WriteThreadCallable(WriteThread writeThread, Map<EqualishExceptionWrapper,Integer> errors,
                                    Runnable fatalExceptionCallback)
        {
            ArgumentValidation.notNull("write thread", writeThread);
            this.writeThread = writeThread;
            this.errors = errors;
            this.fatalExceptionCallback = fatalExceptionCallback;
        }

        @Override
        public Void call() throws InvalidOperationException, InterruptedException {
            DDLFunctions ddl = ddl();
            DMLFunctions dml = dml();
            Session session = ServiceManagerImpl.newSession();
            try {
                writeThread.setupWrites(ddl, dml, session);
                setupSucceeded = true;
            } finally {
                setupDoneLatch.countDown();
            }

            boolean exceptionsNotFatal = true;
            while (exceptionsNotFatal && keepGoing.get()) {
                try {
                    writeThread.ongoingWrites(ddl(), dml(), ServiceManagerImpl.newSession(), keepGoing);
                } catch (Throwable t) {
                    addError(t, errors);
                    exceptionsNotFatal = writeThread.continueThroughException(t);
                    if (!exceptionsNotFatal && fatalExceptionCallback != null) {
                        fatalExceptionCallback.run();
                    }
                }
            }
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
        private final Session session = ServiceManagerImpl.newSession();
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
                requestStruct = hapiReadThread.pullRequest(new ThreadlessRandom(this.hashCode()));
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
            final AtomicBoolean writeThreadAlive = new AtomicBoolean(true);

            WriteThreadCallable writeThreadCallable = new WriteThreadCallable(writeThread, errors, new Runnable() {
                @Override
                public void run() {
                    writeThreadAlive.set(false);
                }
            });
            Future<Void> writeThreadFuture = executor.submit(writeThreadCallable);
            boolean setupSuccess = writeThreadCallable.waitForSetup();
            if (!setupSuccess) {
                writeThreadFuture.get();
                fail("setupSuccess was false, so we should have gotten an ExecutionException");
            }

            Map<EqualishExceptionWrapper,Integer> errorsMap
                    = feedReadThreads(readThreads, executor, errors, writeThreadAlive);
            LOG.debug("test finishing at time {}", System.currentTimeMillis());

            writeThreadCallable.stopOngoingWrites();
            writeThreadFuture.get(5, TimeUnit.SECONDS);

            if (!errorsMap.isEmpty()) {
                failWithErrors(errorsMap);
            }
        } catch (Exception e) {
            throw new RunThreadsException(e);
        }
    }

    private Map<EqualishExceptionWrapper,Integer> feedReadThreads(HapiReadThread[] readThreads,
                                                                  ExecutorService executorService,
                                                                  Map<EqualishExceptionWrapper, Integer> errors,
                                                                  AtomicBoolean writeThreadAlive)
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
        while (randomThreads.hasWeights() && writeThreadAlive.get()) {
            HapiReadThread hapiReadThread = randomThreads.get(-1);
            HapiThreadCallable callable = new HapiThreadCallable(hapiReadThread, "yo");
            Future<Void> submitFuture = executorService.submit(callable);
            submitFutures.put(submitFuture);
        }

        // Tell the processors that we're done, and wait for them to finish
        for (ReadThreadProcessor processor : processors) {
            processor.stopProcessing();
        }
        for (ReadThreadProcessor processor : processors) {
            // If any processor had been starved, it will wait forever. We can flush these by putting in dummy
            // futures, one per processor. If any processors hadn't been starved, they'll just process the dummy,
            // which is fine.
            submitFutures.offer(new DummyFuture());
        }
        for (Future<Void> processingFuture : processingFutures) {
            try {
                processingFuture.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        final int remainingThreads = submitFutures.size();
        // Gobble up any remaining read threads
        int loopsRequired = 0;
        while(!submitFutures.isEmpty()) {
            ++loopsRequired;
            ReadThreadProcessor processor = new ReadThreadProcessor(submitFutures, errors);
            Future<Void> processingFuture = processingService.submit(processor);
            processor.stopProcessing();
            if (submitFutures.isEmpty()) {
                submitFutures.offer(new DummyFuture());
            }
            try {
                processingFuture.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.debug("required {} loop(s) to flush {} remaining thread(s)", loopsRequired, remainingThreads);

        return errors;
    }

    private static class DummyFuture implements Future<Void> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Void get() {
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit) {
            return null;
        }
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

        List<Map.Entry<EqualishExceptionWrapper, Integer>> sorted = sortedEntries(errors);
        summary(sorted, printer);
        separator(printer);

        for (Map.Entry<EqualishExceptionWrapper,Integer> pair : sorted) {
            Throwable error = pair.getKey().get();
            int count = pair.getValue();

            printer.format("%d instance", count);
            if (count != 1) {
                printer.print('s');
            }
            printer.format(" of this general pattern:\n");

            error.printStackTrace(printer);
            separator(printer);
        }
        printer.flush();
        stringWriter.flush();
        fail(stringWriter.toString());
    }

    private static List<Map.Entry<EqualishExceptionWrapper, Integer>> sortedEntries(
            Map<EqualishExceptionWrapper, Integer> errors)
    {
        List<Map.Entry<EqualishExceptionWrapper, Integer>> list
                = new ArrayList<Map.Entry<EqualishExceptionWrapper, Integer>>( errors.size() );
        list.addAll(errors.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<EqualishExceptionWrapper, Integer>>() {
            @Override
            public int compare(
                    Map.Entry<EqualishExceptionWrapper, Integer> o1,
                    Map.Entry<EqualishExceptionWrapper, Integer> o2)
            {
                // no need to check for nulls, I know there won't be any.
                // also, note we're purposely sorting in descending order
                int i1 = o1.getValue();
                int i2 = o2.getValue();
                if (i1 != i2) {
                    return i2 - i1;
                }
                String c1 = o1.getKey().get().getClass().getName();
                String c2 = o2.getKey().get().getClass().getName();
                return c2.compareTo(c1);
            }
        });
        return list;
    }

    private static void summary(List<Map.Entry<EqualishExceptionWrapper, Integer>> errors, PrintWriter printer) {
        int pairsCount = errors.size();
        printer.format("%d failure pattern", pairsCount);
        if (pairsCount != 1) {
            printer.print('s');
        }
        printer.println(':');

        int highest = 0;
        int total = 0;
        for (Map.Entry<EqualishExceptionWrapper, Integer> entry : errors) {
            int v = entry.getValue();
            total += v;
            if (v > highest) {
                highest = v;
            }
        }
        // eg, if the highest number is 3 digits, this will produce the format string: "\t%3d (%7.03f%%): "
        final String totalsFormat = String.format("\t%%%dd (%%7.03f%%%%): ", Integer.toString(highest).length());

        for (Map.Entry<EqualishExceptionWrapper,Integer> pair : errors) {
            int count = pair.getValue();
            float percent = (((float)count) / total) * 100;
            printer.printf(totalsFormat, count, percent);
            Throwable throwable = pair.getKey().get();

            if (HapiReadThread.UnexpectedException.class.equals(throwable.getClass())) {
                // unwrap
                throwable = throwable.getCause();
            }
            while (throwable != null) {
                printer.print(throwable.getClass().getSimpleName());
                StackTraceElement[] frames = throwable.getStackTrace();
                if (frames != null && frames.length > 0) {
                    StackTraceElement frame = frames[0];
                    printer.printf(" (%s:%d)", frame.getFileName(), frame.getLineNumber());
                }
                throwable = throwable.getCause();
                if (throwable != null) {
                    printer.print(" -> ");
                }
            }
            printer.println();
        }
    }

    private static void separator(PrintWriter printer) {
        for (int i=0; i<72; ++i) {
            printer.append('~');
        }
        printer.println();
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
