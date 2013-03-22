package com.akiban.server.service.restdml;

import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;

public class RestQueryContext extends SimpleQueryContext {

    public RestQueryContext(StoreAdapter adapter) {
        super(adapter);
    }
    
    @Override
    public long sequenceNextValue(TableName sequence) {
        return getStore().sequenceNextValue(sequence);

    }

    @Override
    public long sequenceCurrentValue(TableName sequence) {
        return getStore().sequenceCurrentValue(sequence);
    }
}
