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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.sql.parser.ValueNode;

public class BooleanConstantExpression extends ConstantExpression 
                                       implements ConditionExpression 
{
    public BooleanConstantExpression(Object value, 
                                     DataTypeDescriptor sqlType, ValueNode sqlSource,
                                     TInstance type) {
        super(value, sqlType, sqlSource, type);
    }

    public BooleanConstantExpression(Boolean value) {
        super(value, AkBool.INSTANCE.instance(true));
    }
    
    @Override
    public Implementation getImplementation() {
        return null;
    }

}
