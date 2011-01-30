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
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.api.HapiProcessor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public final class RawByteOutputter implements HapiProcessor.Outputter{
    private static final RawByteOutputter instance = new RawByteOutputter();

    public static RawByteOutputter instance() {
        return instance;
    }

    private RawByteOutputter() {}

    @Override
    public void output(RowDefCache rowDefCache, List<RowData> rows, OutputStream outputStream) throws IOException {
        for(RowData data : rows) {
            outputStream.write(data.getBytes(), data.getRowStart(), data.getRowSize());
        }
    }
}
