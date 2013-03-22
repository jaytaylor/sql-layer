
package com.akiban.server.test.it.routines;

import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.BindingNotSetException;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.QueryCanceledException;

import java.sql.Types;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

/** A loadable direct object plan that returns asynchronous results.
 * Needs to use copy mode to get results out as it goes.
 * <code><pre>
CALL sqlj.install_jar('target/akiban-server-1.4.3-SNAPSHOT-tests.jar', 'testjar', 0);
CREATE PROCEDURE system.`exec`(IN cmd VARCHAR(1024)) LANGUAGE java PARAMETER STYLE akiban_loadable_plan EXTERNAL NAME 'testjar:com.akiban.server.test.it.routines.TestDirectAsync';
CALL system.`exec`('tail', '-f', '/tmp/akiban_server/server.log');
 * </pre></code> 
 */
public class TestDirectAsync extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan()
    {
        return new DirectObjectPlan() {
                @Override
                public DirectObjectCursor cursor(QueryContext context) {
                    return new TestDirectObjectCursor(context);
                }

                @Override
                public OutputMode getOutputMode() {
                    return OutputMode.COPY_WITH_NEWLINE;
                }
            };
    }

    public static class TestDirectObjectCursor extends DirectObjectCursor {
        QueryContext context;
        Process process;
        BlockingQueue<String> output;

        public TestDirectObjectCursor(QueryContext context) {
            this.context = context;
        }

        @Override
        public void open() {
            List<String> command = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String carg;
                try {
                    carg = context.getValue(i).getString();
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
