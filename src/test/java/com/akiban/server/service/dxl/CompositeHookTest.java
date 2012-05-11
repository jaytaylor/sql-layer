/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.dxl;

import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.TestSessionFactory;
import com.akiban.util.MultipleCauseException;
import com.akiban.util.Strings;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public final class CompositeHookTest {

    private List<String> output;
    private Session session;

    @Before
    public void setUp() {
        output = new ArrayList<String>();
        session = TestSessionFactory.get().createSession();
    }

    @Test
    public void noExceptions() {
        DXLFunctionsHook hook = compose(output, "alpha", "beta", "gamma");

        hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
        hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_AIS, null);

        check(
                "alpha into GET_AIS",
                "beta into GET_AIS",
                "gamma into GET_AIS",

                "gamma out of GET_AIS",
                "beta out of GET_AIS",
                "alpha out of GET_AIS"
        );
    }

    @Test
    public void wrappedThrowsException() {
        DXLFunctionsHook hook = compose(output, "alpha", "beta", "gamma");

        MySampleException e = new MySampleException();
        hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.CREATE_TABLE);
        hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.CREATE_TABLE, e);
        hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.CREATE_TABLE, e);

        check(
                "alpha into CREATE_TABLE",
                "beta into CREATE_TABLE",
                "gamma into CREATE_TABLE",

                "gamma caught MySampleException in CREATE_TABLE",
                "beta caught MySampleException in CREATE_TABLE",
                "alpha caught MySampleException in CREATE_TABLE",

                "gamma out of CREATE_TABLE",
                "beta out of CREATE_TABLE",
                "alpha out of CREATE_TABLE"
        );
    }

    @Test
    public void crashOnIn() {
        DXLFunctionsHook hook = compose(output, "alpha", "beta: CRASH_IN", "gamma");

        try {
            hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
            fail();
        } catch (MySampleCash e) {
            // good
        }
        MySampleException e = new MySampleException();
        hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
        hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);

        check(
                "alpha into GET_AIS",
                "beta: CRASH_IN into GET_AIS",

                "beta: CRASH_IN caught MySampleException in GET_AIS",
                "alpha caught MySampleException in GET_AIS",

                "beta: CRASH_IN out of GET_AIS",
                "alpha out of GET_AIS"
        );
    }

    @Test
    public void crashOnCatch() {
        DXLFunctionsHook hook = compose(output, "alpha", "beta: CRASH_CATCH", "gamma");

        hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
        MySampleException e = new MySampleException();
        try {
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
            fail();
        } catch (MySampleCash e1) {
            // good
        }
        hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);

        check(
                "alpha into GET_AIS",
                "beta: CRASH_CATCH into GET_AIS",
                "gamma into GET_AIS",

                "gamma caught MySampleException in GET_AIS",
                "beta: CRASH_CATCH caught MySampleException in GET_AIS",
                "alpha caught MySampleException in GET_AIS",

                "gamma out of GET_AIS",
                "beta: CRASH_CATCH out of GET_AIS",
                "alpha out of GET_AIS"
        );
    }

    @Test
    public void crashOnFinally() {
        DXLFunctionsHook hook = compose(output, "alpha", "beta: CRASH_FINALLY", "gamma");

        MySampleException e = new MySampleException();
        hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
        hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
        try {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
            fail();
        } catch (MySampleCash e1) {
            // good
        }

        check(
                "alpha into GET_AIS",
                "beta: CRASH_FINALLY into GET_AIS",
                "gamma into GET_AIS",

                "gamma caught MySampleException in GET_AIS",
                "beta: CRASH_FINALLY caught MySampleException in GET_AIS",
                "alpha caught MySampleException in GET_AIS",

                "gamma out of GET_AIS",
                "beta: CRASH_FINALLY out of GET_AIS",
                "alpha out of GET_AIS"
        );
    }

    @Test
    public void multipleCrashes() {
        DXLFunctionsHook hook = compose(output,
                "alpha",
                "beta: CRASH_CATCH CRASH_FINALLY",
                "gamma: CRASH_CATCH CRASH_FINALLY",
                "delta"
        );

        hook.hookFunctionIn(session, DXLFunctionsHook.DXLFunction.GET_AIS);
        MySampleException e = new MySampleException();
        try {
            hook.hookFunctionCatch(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
            fail();
        } catch (MultipleCauseException e1) {
            assertEquals("causes", 2, e1.getCauses().size());
            // good
        }

        try {
            hook.hookFunctionFinally(session, DXLFunctionsHook.DXLFunction.GET_AIS, e);
            fail();
        } catch (MultipleCauseException e1) {
            assertEquals("causes", 2, e1.getCauses().size());
            // good
        }

        check(
                "alpha into GET_AIS",
                "beta: CRASH_CATCH CRASH_FINALLY into GET_AIS",
                "gamma: CRASH_CATCH CRASH_FINALLY into GET_AIS",
                "delta into GET_AIS",

                "delta caught MySampleException in GET_AIS",
                "gamma: CRASH_CATCH CRASH_FINALLY caught MySampleException in GET_AIS",
                "beta: CRASH_CATCH CRASH_FINALLY caught MySampleException in GET_AIS",
                "alpha caught MySampleException in GET_AIS",

                "delta out of GET_AIS",
                "gamma: CRASH_CATCH CRASH_FINALLY out of GET_AIS",
                "beta: CRASH_CATCH CRASH_FINALLY out of GET_AIS",
                "alpha out of GET_AIS"
        );

    }

    private void check(String... expected) {
        // if this fails, joining the lists makes it easier to diff
        assertEquals("messages", Strings.join(Arrays.asList(expected)), Strings.join(output));
        // sanity check that our lists are really really equal, not just equivalent toString
        assertEquals("messages", Arrays.asList(expected), output);
    }

    DXLFunctionsHook compose(List<String> output, String... messages) {
        List<DXLFunctionsHook> hooks = new ArrayList<DXLFunctionsHook>();
        for (String message : messages) {
            hooks.add( new ToListHook(message, output) );
        }
        return new CompositeHook(hooks);
    }

    private static class ToListHook implements DXLFunctionsHook {
        private final String message;
        private final List<String> output;

        private ToListHook(String message, List<String> output) {
            this.output = output;
            this.message = message;
        }

        @Override
        public void hookFunctionIn(Session session, DXLFunction function) {
            output.add(String.format("%s into %s", message, function.name()));
            if (message.contains("CRASH_IN")) {
                throw new MySampleCash();
            }
        }

        @Override
        public void hookFunctionCatch(Session session, DXLFunction function, Throwable throwable) {
            output.add(
                    String.format(
                            "%s caught %s in %s",
                            message,
                            throwable.getClass().getSimpleName(),
                            function.name()
                    )
            );
            if (message.contains("CRASH_CATCH")) {
                throw new MySampleCash();
            }
        }

        @Override
        public void hookFunctionFinally(Session session, DXLFunction function, Throwable thrown) {
            output.add(String.format("%s out of %s", message, function.name()));
            if (message.contains("CRASH_FINALLY")) {
                throw new MySampleCash();
            }
        }
    }

    private static class MySampleException extends Exception {

    }

    private static class MySampleCash extends RuntimeException {

    }
}
