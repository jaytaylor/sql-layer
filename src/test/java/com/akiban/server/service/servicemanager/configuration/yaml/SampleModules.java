package com.akiban.server.service.servicemanager.configuration.yaml;

import com.google.inject.AbstractModule;

@SuppressWarnings("unused") // via test-bind-modules
public final class SampleModules {

    public static class GoodModule extends AbstractModule {
        @Override
        protected void configure() {

        }
    }

    public static class NotAModule {
        protected void configure() {

        }
    }

    public static class ArgCtorModule extends AbstractModule {
        protected void configure() {

        }

        public ArgCtorModule(int i) {}
    }

    public static class PrivateCtorModule extends AbstractModule {
        protected void configure() {

        }

        private PrivateCtorModule() {}
    }

    private SampleModules() {}
}
