/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
