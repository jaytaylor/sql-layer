package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;

public interface SortKeySource<S> {
    void attach(Key key, int i, AkType fieldType, TInstance tInstance);
    S asSource();
}
