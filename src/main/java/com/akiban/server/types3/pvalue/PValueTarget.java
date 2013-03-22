
package com.akiban.server.types3.pvalue;

public interface PValueTarget extends PBasicValueTarget {

    boolean supportsCachedObjects();

    void putObject(Object object);
}
