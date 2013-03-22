
package com.akiban.server.types3;

import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.pvalue.PUnderlying;

public class T3TestClass extends NoAttrTClass {

    public T3TestClass(String name) {
        super(bundle, name, TestCategory.ONLY, null, 1, 1, 1, PUnderlying.INT_64, null, 64, null);
    }

    public enum TestClassCategory {
        ONLY
    }

    public static final TBundleID bundle = new TBundleID("testbundle", "8b298621-fed8-48ca-b33c-dd3a3905f72e");

    public enum TestCategory {
        ONLY
    }
}
