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
import com.akiban.server.RowData;
import com.akiban.server.RowDefCache;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.util.ArgumentValidation;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("unused") // via JMX
public class Fetchrows implements HapiProcessor, JmxManageable {
    private static final Fetchrows instance = new Fetchrows();

    public static Fetchrows instance() {
        return instance;
    }

    private static final Class<?> MODULE = Fetchrows.class;
    private static final String SESSION_BUFFER = "SESSION_BUFFER";

    private final AtomicInteger capacity = new AtomicInteger(65536);

    private Fetchrows()
    {}

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("HapiP-Fetchrows", new FetchrowsMXBean() {
            @Override
            public int getBufferCapacity() {
                return capacity.get();
            }

            @Override
            public void setBufferCapacity(int bytes) {
                capacity.set(bytes);
            }
        },
        FetchrowsMXBean.class);
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request, HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException
    {
        Store storeLocal = ServiceManagerImpl.get().getStore();
        if (storeLocal == null) {
            throw new HapiRequestException("Service not started (Store is null",
                    HapiRequestException.ReasonCode.INTERNAL_ERROR
            );
        }

        ByteBuffer buffer = session.get(MODULE, SESSION_BUFFER);
        if (buffer == null) {
            buffer = ByteBuffer.allocate(capacity.get());
            session.put(MODULE, SESSION_BUFFER, buffer);
        }
        else {
            buffer.clear();
        }

        doProcessRequest(storeLocal, session, request, buffer, outputter, outputStream);
    }



    private static void doProcessRequest(Store store, Session session, HapiGetRequest request,
                                         ByteBuffer byteBuffer, HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException
    {
        ArgumentValidation.notNull("outputter", outputter);
        HapiGetRequest.Predicate predicate = extractLimitedPredicate(request);

        final RowDefCache cache = store.getRowDefCache();
        final List<RowData> list;
        try {
            list = store.fetchRows(
                    session,
                    request.getSchema(),
                    request.getTable(),
                    predicate.getColumnName(),
                    predicate.getValue(),
                    predicate.getValue(),
                    null,
                    byteBuffer
            );
        } catch (Exception e) {
            throw new HapiRequestException("while fetching rows", e);
        }

        try {
            outputter.output(new DefaultProcessedRequest(request, session), list, outputStream);
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, HapiRequestException.ReasonCode.WRITE_ERROR);
        }
    }

    private static HapiGetRequest.Predicate extractLimitedPredicate(HapiGetRequest request) throws HapiRequestException {
        if (request.getPredicates().size() != 1) {
            complain("You may only specify one predicate (for now!) -- saw %s", request.getPredicates());
        }
        if (!request.getTable().equals(request.getUsingTable().getTableName())) {
            complain("You may not specify a different SELECT table and USING table (for now!) -- %s != %s",
                    request.getTable(), request.getUsingTable().getTableName());
        }
        HapiGetRequest.Predicate predicate = request.getPredicates().iterator().next();
        if (!predicate.getTableName().equals(request.getUsingTable())) {
            complain("Can't have different SELECT table and predicate table (for now!) %s != %s",
                    predicate.getTableName(), request.getUsingTable()
            );
        }
        return predicate;
    }

    private static void complain(String format, Object... args) throws HapiRequestException {
        throw new HapiRequestException(String.format(format, args),
                HapiRequestException.ReasonCode.UNSUPPORTED_REQUEST
        );
    }

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException {
        return null;
    }
}
