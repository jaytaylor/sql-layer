
package com.akiban.server.service.servicemanager;

/**
 * Alpha <- Beta <- Gamma <- nothing
 */
final class DummyErroringServices {

    public static class ErroringAlpha implements DummyInterfaces.Alpha {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Beta.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
        }

        @Override
        public void stop() {}
    }

    public static class ErroringBeta implements DummyInterfaces.Beta {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Gamma.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
            throw new ErroringException();
        }

        @Override
        public void stop() {}
    }

    public static class ErroringGamma implements DummyInterfaces.Gamma {
        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    static class ErroringException extends RuntimeException {}
}
