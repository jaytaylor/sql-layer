
package com.akiban.server.store.statistics;

import com.akiban.ais.model.Index;
import com.akiban.server.service.tree.KeyCreator;
import com.persistit.Key;
import com.persistit.Value;

class MultiColumnIndexStatisticsVisitor extends IndexStatisticsGenerator
{
    @Override
    public void visit(Key key, Value value)
    {
        loadKey(key);
    }

    public MultiColumnIndexStatisticsVisitor(Index index, KeyCreator keyCreator)
    {
        super(index, index.getKeyColumns().size(), -1, keyCreator);
    }
}
