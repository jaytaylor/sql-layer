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

package com.akiban.server.service.memcache.outputter;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.service.jmx.JmxManageable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

public final class DummyOutputter implements HapiOutputter, JmxManageable {

    @SuppressWarnings("unused") // jmx
    public interface DummyOutputterMXBean {
        String getDummyText();
        void setDummyText(String text);
    }

    private static final DummyOutputter instance= new DummyOutputter();

    public static DummyOutputter instance() {
        return instance;
    }

    private final AtomicReference<String> string = new AtomicReference<String>("DUMMY DATA");
    private DummyOutputter() {}

    @Override
    public void output(HapiProcessedGetRequest request,
                       boolean hKeyOrdered,
                       Iterable<RowData> rows,
                       OutputStream outputStream) throws IOException {
        outputStream.write(string.get().getBytes());
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("HapiO-Dummy", new DummyOutputterMXBean() {
            @Override
            public String getDummyText() {
                return string.get();
            }

            @Override
            public void setDummyText(String text) {
                string.set(text);
            }
        },
        DummyOutputterMXBean.class);
    }
}
