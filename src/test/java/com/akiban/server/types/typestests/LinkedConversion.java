
package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

import java.util.Set;

public interface LinkedConversion<T> {
    ValueSource linkedSource();
    ValueTarget linkedTarget();

    void checkPut(T expected);
    void setUp(TestCase<?> testCase);
    void syncConversions();

    Set<? extends AkType> unsupportedTypes();
}
