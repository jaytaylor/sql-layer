
package com.akiban.server.expression;

import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import java.util.ArrayList;

public abstract class TypesList extends ArrayList<ExpressionType>
{
    protected TypesList (int size)
    {
        super(size);
    }
    
    abstract public void setType(int index, AkType newType) throws StandardException;
}
