package com.akiban.qp.persistitadapter.indexcursor;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.persistit.Key;

public interface SortKeyTarget<S> {
    void attach(Key key);
    void append(S source, int f, AkType[] akTypes, TInstance[] tInstances, AkCollator[] collators);
    void append(S source, AkType akType, TInstance tInstance, AkCollator collator);
    void append(S source, AkCollator collator, TInstance tInstance);
}
