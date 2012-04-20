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

package com.akiban.server.service.servicemanager;

import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.util.Strings;
import com.google.inject.ProvisionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class GuicerTest {

    @Before
    public void clearMessages() {
        DummyInterfaces.clearMessages();
    }

    @Test
    public void simple() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummySimpleServices.SimpleAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummySimpleServices.SimpleBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        startRequiredServices(guicer);
        assertEquals(
                "messages",
                joined(
                        "starting SimpleAlpha",
                        "starting SimpleBeta",
                        "starting SimpleGamma",
                        "started SimpleGamma",
                        "started SimpleBeta",
                        "started SimpleAlpha"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    @Test
    public void mixedDI() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummyMixedDIServices.MixedDIAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummyMixedDIServices.MixedDIBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummyMixedDIServices.MixedDIGamma.class, false)
        );
        startRequiredServices(guicer);
        assertEquals(
                "messages",
                joined(
                        "starting MixedDIBeta",
                        "starting MixedDIGamma",
                        "started MixedDIGamma",
                        "started MixedDIBeta",
                        "starting MixedDIAlpha",
                        "started MixedDIAlpha"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    @Test
    public void errorOnStartup() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummyErroringServices.ErroringAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummyErroringServices.ErroringBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummyErroringServices.ErroringGamma.class, false)
        );
        try{
            startRequiredServices(guicer);
            fail("should have caught ErroringException");
        } catch (ProvisionException e) {
            assertEventualCause(e, DummyErroringServices.ErroringException.class);
            assertEquals(
                    "messages",
                    joined(
                            "starting ErroringAlpha",
                            "starting ErroringBeta",
                            "starting ErroringGamma",
                            "started ErroringGamma",
                            "stopping ErroringGamma",
                            "stopped ErroringGamma"
                    ),
                    Strings.join(DummyInterfaces.messages())
            );
        }
    }

    @Test
    public void singletonNess() throws Exception {
        Guicer guicer = messageGuicer(
                bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        guicer.get(DummyInterfaces.Gamma.class, MESSAGING_ACTIONS);
        guicer.get(DummyInterfaces.Gamma.class, MESSAGING_ACTIONS);
        assertEquals(
                "messages",
                joined(
                        "starting SimpleGamma",
                        "started SimpleGamma"
                ),
                Strings.join(DummyInterfaces.messages())
        );
    }

    private void startRequiredServices(Guicer guicer) {
        for (Class<?> clazz : guicer.directlyRequiredClasses()) {
            guicer.get(clazz, MESSAGING_ACTIONS);
        }
    }

    private void assertEventualCause(Throwable e, Class<? extends Throwable> exceptionClassToFind) {
        Throwable cause = e;
        while (cause != null) {
            if (cause.getClass().equals(exceptionClassToFind)) {
                return;
            }
            cause = cause.getCause();
        }
        fail(exceptionClassToFind + " was not in the causes of " + e);
    }

    private static Guicer messageGuicer(ServiceBinding... bindings) throws ClassNotFoundException {
        return onlyGuicer = Guicer.forServices(Arrays.asList(bindings));
    }

    private static <T> ServiceBinding bind(Class<T> theInterface, Class<? extends T> theClass, boolean required) {
        if (!theInterface.isInterface()) {
            throw new IllegalArgumentException("theInterface must be an interface class: " + theInterface);
        }
        if (theClass.isInterface()) {
            throw new IllegalArgumentException("theClass must not be an interface class: " + theClass);
        }
        ServiceBinding binding = new ServiceBinding(theInterface.getName());
        binding.setImplementingClass(theClass.getName());
        if (required) {
            binding.markDirectlyRequired();
        }
        return binding;
    }

    private static String joined(String... strings) {
        return Strings.join(Arrays.asList(strings));
    }

    // For use within package

    static Guicer onlyGuicer() {
        if (onlyGuicer == null) {
            throw new IllegalStateException("no guicer set");
        }
        return onlyGuicer;
    }

    // class state

    static final Guicer.ServiceLifecycleActions<DummyInterfaces.DummyService> MESSAGING_ACTIONS
            = new Guicer.ServiceLifecycleActions<DummyInterfaces.DummyService>()
    {
        @Override
        public void onStart(DummyInterfaces.DummyService service) {
            DummyInterfaces.addMessage("starting " + service.getClass().getSimpleName());
            service.start();
            DummyInterfaces.addMessage("started " + service.getClass().getSimpleName());
        }

        @Override
        public void onShutdown(DummyInterfaces.DummyService service) {
            DummyInterfaces.addMessage("stopping " + service.getClass().getSimpleName());
            service.stop();
            DummyInterfaces.addMessage("stopped " + service.getClass().getSimpleName());
        }

        @Override
        public DummyInterfaces.DummyService castIfActionable(Object object) {
            return (object instanceof DummyInterfaces.DummyService) ? (DummyInterfaces.DummyService) object : null;
        }
    };

    private static Guicer onlyGuicer;
}
