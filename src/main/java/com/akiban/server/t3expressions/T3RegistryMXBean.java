
package com.akiban.server.t3expressions;

public interface T3RegistryMXBean {
    String describeTypes();
    String describeCasts();
    String describeScalars();
    String describeAggregates();
    String describeAll();
    boolean isNewtypesOn();
}
