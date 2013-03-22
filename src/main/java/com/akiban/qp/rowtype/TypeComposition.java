
package com.akiban.qp.rowtype;


import com.akiban.ais.model.UserTable;
import com.akiban.util.ArgumentValidation;

import java.util.*;

public class TypeComposition
{
    public boolean isAncestorOf(TypeComposition that)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isParentOf(TypeComposition that)
    {
        throw new UnsupportedOperationException();
    }

    public final Set<UserTable> tables()
    {
        return tables;
    }

    public TypeComposition(RowType rowType, Collection<UserTable> tables)
    {
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notEmpty("tables", tables);
        this.rowType = rowType;
        this.tables = Collections.unmodifiableSet(new HashSet<>(tables));
    }

    // Object state

    protected final RowType rowType;
    protected final Set<UserTable> tables;
}
