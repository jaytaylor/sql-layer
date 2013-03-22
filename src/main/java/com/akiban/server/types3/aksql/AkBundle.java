
package com.akiban.server.types3.aksql;

import com.akiban.server.types3.TBundle;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClass;

import java.util.Map;

public enum AkBundle implements TBundle {
    INSTANCE;

    @Override
    public TBundleID id() {
        return bundleId;
    }

    private static TBundleID bundleId = new TBundleID("aksql", "282696ac-6f10-450c-9960-a54c8abe94c0");
}
