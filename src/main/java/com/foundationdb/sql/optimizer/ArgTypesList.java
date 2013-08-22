/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer;

import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.types.AkType;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.optimizer.FunctionsTypeComputer.ArgumentsAccess;

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
