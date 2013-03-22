
package com.akiban.server.test.it.pstraverse;

import org.junit.Test;

import java.util.Arrays;

public final class CascadingKeysTraversalIT extends KeysBase {
    @Override
    protected String ordersPK() {
        return "cid,oid";
    }

    @Override
    protected String itemsPK() {
        return "cid,oid,iid";
    }

    @Override
    @Test @SuppressWarnings("unused") // junit will invoke
    public void traverseOrdersPK() throws Exception {
        traversePK(
                orders(),
                Arrays.asList(71L, 81L),
                Arrays.asList(72L, 82L)
        );
    }

    @Override
    @Test @SuppressWarnings("unused") // junit will invoke
    public void traverseItemsPK() throws Exception {
        traversePK(
                items(),
                Arrays.asList(71L, 81L, 91L),
                Arrays.asList(71L, 81L, 92L),
                Arrays.asList(72L, 82L, 93L)
        );
    }
}
