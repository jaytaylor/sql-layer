
package com.akiban.server.test.it.pstraverse;

public final class StandardKeysTraversalIT extends KeysBase {
    @Override
    protected String ordersPK() {
        return "oid";
    }

    @Override
    protected String itemsPK() {
        return "iid";
    }
}
