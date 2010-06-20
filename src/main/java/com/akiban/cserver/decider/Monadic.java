/**
 * 
 */
package com.akiban.cserver.decider;

import com.akiban.cserver.RowDef;
import com.akiban.cserver.message.ScanRowsRequest;

/**
 * @author percent
 * 
 */
public class Monadic implements Decider {
    public Monadic(Decider.RowCollectorType type) {
        engineType = type;
    }

    @Override
    public RowCollectorType decide(ScanRowsRequest r, RowDef rowDef) {
        if (rowDef != null) {
            if (rowDef.getSchemaName().equals("toy_test")) {
                return engineType;
            } else {
                return Decider.RowCollectorType.PersistitRowCollector;
            }
        } else {
            return engineType;
        }
    }

    public Decider.RowCollectorType engineType;
}
