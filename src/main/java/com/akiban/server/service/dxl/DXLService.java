
package com.akiban.server.service.dxl;

import com.akiban.ais.model.GroupIndex;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;

public interface DXLService {
    DDLFunctions ddlFunctions();
    DMLFunctions dmlFunctions();
    void recreateGroupIndexes(GroupIndexRecreatePredicate predicate);

    public interface GroupIndexRecreatePredicate {
        boolean shouldRecreate(GroupIndex index);
    }
}
