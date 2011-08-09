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

package com.akiban.server.service.memcache.hprocessor;

import com.akiban.ais.model.Index;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.memcache.HapiProcessorFactory;
import com.akiban.server.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CachedProcessor implements HapiProcessor, JmxManageable {
    private final AtomicReference<HapiProcessorFactory> processorFactory;

    private final HapiCacheMXBean bean = new HapiCacheMXBean() {
        @Override
        public HapiProcessorFactory getDelegateProcessor() {
            return processorFactory.get();
        }

        @Override
        public void setDelegateProcessor(HapiProcessorFactory delegateProcessor) {
            if (delegateProcessor.getHapiProcessor() instanceof CachedProcessor) {
                throw new IllegalArgumentException("Can't cache using a HapiCache factory");
            }
            processorFactory.set(delegateProcessor);
        }
    };

    public CachedProcessor() {
        this.processorFactory = new AtomicReference<HapiProcessorFactory>(HapiProcessorFactory.SCANROWS);
    }

    private HapiProcessor delegate() {
        return processorFactory.get().getHapiProcessor();
    }

    private HapiGetRequest lastRequest = null;
    private ReplayOutputter lastOutputter = null;
    private HapiProcessorFactory lastProcessorFactory = null;
    private HapiProcessor lastDelegate = null;
    
    private final Object MONITOR = new Object();

    @Override
    public void processRequest(Session session, HapiGetRequest request, HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException {
        final HapiProcessorFactory factory = processorFactory.get();
        final ReplayOutputter replayOutputter;
        synchronized (MONITOR) {
            if (    lastOutputter == null
                    || lastRequest == null
                    || !lastRequest.equals(request)
                    || !factory.equals(lastProcessorFactory)
            ) {
                replayOutputter = new ReplayOutputter();
                if (!factory.equals(lastProcessorFactory)) {
                    lastProcessorFactory = factory;
                    lastDelegate = factory.getHapiProcessor();
                }
                lastDelegate.processRequest(session, request, replayOutputter, null);
                requestWasProcessed();
                lastRequest = request;
                lastOutputter = replayOutputter;

            }
            else {
                replayOutputter = lastOutputter;
            }
        }
        try {
            replayOutputter.replay(outputter, outputStream);
        } catch (IOException e) {
            throw new HapiRequestException("While replaying output", e);
        }
    }

    protected void requestWasProcessed() {
    }

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException {
        return delegate().findHapiRequestIndex(session, request);
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("H-Cache", bean, HapiCacheMXBean.class);
    }

    private static class ReplayOutputter implements HapiOutputter {
        private final ReplayOutputter MONITOR = this;
        private List<RowData> cachedRows;
        private HapiProcessedGetRequest cachedRequest;

        @Override
        public void output(HapiProcessedGetRequest request,
                           boolean hKeyOrdered,
                           Iterable<RowData> rows,
                           OutputStream ignored) {
            synchronized (MONITOR) {
                assert this.cachedRequest == null : "already have a cached request";
                assert this.cachedRows == null : "already have cached rows: " + this.cachedRows.size();
                this.cachedRequest = request;
                this.cachedRows = copyRows(rows);
            }
        }

        private static List<RowData> copyRows(Iterable<RowData> rows) {
            Map<byte[],byte[]> origToCopies = new HashMap<byte[], byte[]>(1);
            List<RowData> copies = new ArrayList<RowData>();
            for (RowData orig : rows) {
                byte[] origBacking = orig.getBytes();
                byte[] copyBacking = origToCopies.get(origBacking);
                if (copyBacking == null) {
                    copyBacking = Arrays.copyOf(origBacking, origBacking.length);
                    origToCopies.put(origBacking, copyBacking);
                }
                RowData copy = new RowData(copyBacking, orig.getBufferStart(), orig.getBufferLength());
                copy.prepareRow(orig.getRowStart());
                copies.add(copy);
            }
            return Collections.unmodifiableList(copies);
        }

        void replay(HapiOutputter toOutput, OutputStream outputStream) throws IOException {
            final List<RowData> rows;
            final HapiProcessedGetRequest request;
            synchronized (MONITOR) {
                rows = this.cachedRows;
                request = this.cachedRequest;
            }
            toOutput.output(request, true, rows, outputStream);
        }
    }
}
