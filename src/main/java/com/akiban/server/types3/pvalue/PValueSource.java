
package com.akiban.server.types3.pvalue;

public interface PValueSource extends PBasicValueSource {

    boolean hasAnyValue();
    
    boolean hasRawValue();
    
    boolean hasCacheValue();

    boolean canGetRawValue();

    Object getObject();
}
