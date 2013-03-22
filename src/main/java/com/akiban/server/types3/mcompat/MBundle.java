
package com.akiban.server.types3.mcompat;

import com.akiban.server.types3.TBundle;
import com.akiban.server.types3.TBundleID;
import com.akiban.server.types3.TClass;

import java.util.Map;

public enum  MBundle implements TBundle {
    INSTANCE;

    @Override
    public TBundleID id() {
        return bundleId;
    }

    private static TBundleID bundleId = new TBundleID("mcompat", "b9833ebf-423d-4a60-8226-1dd635ba8892");
}
