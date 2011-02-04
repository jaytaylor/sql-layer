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

package com.akiban.cserver.api;
import com.akiban.ais.model.Index;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.service.session.Session;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public interface HapiProcessor {
    public interface Outputter {
        void output(HapiGetRequest request, RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException;
    }
	public void processRequest(Session session, HapiGetRequest request, Outputter outputter, OutputStream outputStream)
            throws HapiRequestException;
    Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException;
}
