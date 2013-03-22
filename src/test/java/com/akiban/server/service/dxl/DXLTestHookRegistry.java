
package com.akiban.server.service.dxl;

public final class DXLTestHookRegistry {

    public static DXLTestHooks get() {
        return INSTANCE;
    }

    private static class SingleMiddlemanHooks implements DXLTestHooks {
        @Override
        public boolean openCursorsExist() {
            return ! middleman().getScanDataMap().isEmpty();
        }

        @Override
        public String describeOpenCursors() {
            return middleman().getScanDataMap().toString();
        }

        private BasicDXLMiddleman middleman() {
            BasicDXLMiddleman middleman = BasicDXLMiddleman.last();
            if (middleman == null) {
                throw new RuntimeException("no active middleman; DXLService probably wasn't started correctly");
            }
            return middleman;
        }
    }

    private static final SingleMiddlemanHooks INSTANCE = new SingleMiddlemanHooks();
}
