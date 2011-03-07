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

package com.akiban.server.mttests.mthapi.coi;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.BasicHapiSuccess;
import com.akiban.server.mttests.mthapi.common.BasicWriter;
import com.akiban.server.service.session.Session;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CoiMT extends HapiMTBase {
    final static int MAX_READ_ID = 10000;
    @Test
    public void preWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(coia(), allowOrphans(), 10000) {
            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                // do nothing
            }
        };
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void concurrentWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(coia(), allowOrphans());
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void concurrentWritesNoOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(coia(), allowOrphans());
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void preWritesNoOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(coia(), allowOrphans()) {
            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                // do nothing
            }
        };
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);
        runThreads(writeThread, readThread);
    }

    private static Set<SaisTable> coia() {
        throw new UnsupportedOperationException();
    }

    private static BasicWriter.RowGenerator allowOrphans() {
        throw new UnsupportedOperationException();
    }
}
