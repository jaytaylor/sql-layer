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

package com.akiban.cserver.service.memcache.hprocessor;

import com.akiban.ais.model.Index;
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

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException {
        return null;
    }
}
