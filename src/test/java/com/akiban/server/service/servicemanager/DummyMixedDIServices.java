
package com.akiban.server.service.servicemanager;

import javax.inject.Inject;

/**
 * Alpha <- Beta <- Gamma <- nothing
 */
final class DummyMixedDIServices {

    public static class MixedDIAlpha implements DummyInterfaces.Alpha {
        @Inject private DummyInterfaces.Beta beta = null;
        @Override
        public void start() {
            assert beta != null;
        }

        @Override
        public void stop() {}
    }

    public static class MixedDIBeta implements DummyInterfaces.Beta {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Gamma.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
        }

        @Override
        public void stop() {}
    }

    public static class MixedDIGamma implements DummyInterfaces.Gamma {
        @Override
        public void start() {}

        @Override
        public void stop() {}
    }
}
