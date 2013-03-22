
package com.akiban.sql.optimizer;

import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.FunctionsTypeComputer.ArgumentsAccess;

class ArgTypesList extends TypesList
{
    private ArgumentsAccess args;

     ArgTypesList (ArgumentsAccess args)
    {
        super(args.nargs());
        this.args = args;
    }

    @Override
    public void setType(int index, AkType newType) throws StandardException
    {
        ExpressionType oldType = get(index);
        if (oldType.getType() != newType)
            set(index, args.addCast(index, oldType, newType));
    }
}
