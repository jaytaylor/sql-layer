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

package com.akiban.cserver.service.memcache.outputter;

import com.akiban.cserver.RowData;
import com.akiban.cserver.api.HapiOutputter;
import com.akiban.cserver.api.HapiProcessedGetRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.List;

public final class RowDataStringOutputter implements HapiOutputter {
    private static final RowDataStringOutputter instance = new RowDataStringOutputter();

    public static RowDataStringOutputter instance() {
        return instance;
    }

    private RowDataStringOutputter() {}
    
    @Override
    public void output(HapiProcessedGetRequest request, List<RowData> rows, OutputStream outputStream) throws IOException {
        PrintWriter writer = new PrintWriter(outputStream);
        for (RowData data : rows) {
            String toString = data.toString(request.getRowDef(data.getRowDefId()));
            writer.println(toString);
        }
        writer.flush();
    }
}
