
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;

public interface TypedPlan {
    int nFields();
    TInstance getTypeAt(int index);
    void setTypeAt(int index, TPreptimeValue value);
}
