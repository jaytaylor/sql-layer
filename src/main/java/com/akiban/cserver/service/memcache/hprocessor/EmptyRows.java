package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

public class EmptyRows implements HapiProcessor {
    private static final EmptyRows instance = new EmptyRows();

    public static EmptyRows instance() {
        return instance;
    }

    private EmptyRows()
    {}

    @Override
    public void processRequest(Session session, HapiGetRequest request, Outputter outputter, OutputStream outputStream) throws HapiRequestException {
        try {
            outputter.output(
                    request,
                    ServiceManagerImpl.get().getStore().getRowDefCache(),
                    new ArrayList<RowData>(),
                    outputStream);
        } catch (IOException e) {
            throw new HapiRequestException("while writing output", e, HapiRequestException.ReasonCode.WRITE_ERROR);
        }
    }
}
