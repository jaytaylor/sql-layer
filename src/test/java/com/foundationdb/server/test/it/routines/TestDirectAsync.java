/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.test.it.routines;

import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.QueryCanceledException;

import java.sql.Types;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

/** A loadable direct object plan that returns asynchronous results.
 * Needs to use copy mode to get results out as it goes.
 * <code><pre>
CALL sqlj.install_jar('target/fdb-sql-layer-x.y.z-tests.jar', 'testjar', 0);
CREATE PROCEDURE system.`exec`(IN cmd VARCHAR(1024)) LANGUAGE java PARAMETER STYLE foundationdb_loadable_plan EXTERNAL NAME 'testjar:com.foundationdb.server.test.it.routines.TestDirectAsync';
CALL system.`exec`('tail', '-f', '/tmp/fdb-sql-layer/layer.log');
 * </pre></code> 
 */
public class TestDirectAsync extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                    return new TestDirectObjectCursor(context, bindings);
                }

                @Override
                public OutputMode getOutputMode() {
                    return OutputMode.COPY_WITH_NEWLINE;
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        QueryContext context;
        QueryBindings bindings;
        Process process;
        BlockingQueue<String> output;

        public TestDirectObjectCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        public void open() {
            List<String> command = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String carg;
                try {
                    carg = bindings.getValue(i).getString();
                    //carg = bindings.getValue(i).getString();
                }
                catch (BindingNotSetException ex) {
                    break;
                }
                command.add(carg);
            }
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            try {
                process = pb.start();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Could not launch", ex);
            }
            output = new LinkedBlockingQueue<>();
            new Thread() {
                private final BufferedReader input = 
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
                @Override
                public void run() {
                    while (true) {
                        try {
                            String line = input.readLine();
                            if (line == null) {
                                output.add(EOF);
                                break;
                            }
                            output.add(line);
                        }
                        catch (IOException ex) {
                            // Could log the error, I suppose.
                            break;
                        }
                    }
                }
            }.start();
        }

        @Override
        public List<String> next() {
            String line;
            try {
                line = output.poll(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ex) {
                throw new QueryCanceledException(context.getSession());
            }
            if (line == null)
                return Collections.<String>emptyList();
            else if (line == EOF)
                return null;
            else
                return Collections.singletonList(line);
        }

        @Override
        public void close() {
            process.destroy();
        }
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
    private static final long TIMEOUT = 500;
    private static final String EOF = "*EOF*";
}
