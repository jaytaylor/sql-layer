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

package com.akiban.server.test.it.loadableplan;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.BindingNotSetException;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.service.session.Session;

import java.sql.Types;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

/** A loadable direct object plan that returns asynchronous results.
 * Not as useful as it seems, since clients actually wait for the complete result set.
 * <code><pre>
java -jar jmxterm-1.0-alpha-4-uber.jar -l localhost:8082 -n <<EOF
run -b com.akiban:type=PostgresServer loadPlan `pwd`/`ls target/akiban-server-*-SNAPSHOT-tests.jar` com.akiban.server.test.it.loadableplan.TestDirectAsync
EOF
psql "host=localhost port=15432 sslmode=disable user=user password=pass" test <<EOF
call "system.exec"('tail', '-f', '/tmp/akiban_server/server.log');
EOF
 * </pre></code> 
 */
public class TestDirectAsync extends LoadableDirectObjectPlan
{
    @Override
    public String name()
    {
        return "system.exec";
    }

    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(Session session) {
                    return new TestDirectObjectCursor(session);
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        Session session;
        Process process;
        BlockingQueue<String> output;

        public TestDirectObjectCursor(Session session) {
            this.session = session;
        }

        @Override
        public void open(Bindings bindings) {
            List<String> command = new ArrayList<String>();
            for (int i = 0; i < 100; i++) {
                String carg;
                try {
                    carg = (String)bindings.get(i);
                }
                catch (BindingNotSetException ex) {
                    break;
                }
                if (carg == null) break;
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
            output = new LinkedBlockingQueue<String>();
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
                throw new QueryCanceledException(session);
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
    public List<String> columnNames() {
        return NAMES;
    }

    @Override
    public int[] jdbcTypes()
    {
        return TYPES;
    }

    private static final List<String> NAMES = Arrays.asList("output");
    private static final int[] TYPES = new int[] { Types.VARCHAR };
    private static final long TIMEOUT = 500;
    private static final String EOF = "*EOF*";
}
