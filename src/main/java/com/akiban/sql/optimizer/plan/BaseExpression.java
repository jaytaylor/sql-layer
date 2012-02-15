/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An evaluated value. 
 * Usually part of a larger expression tree.
*/
public abstract class BaseExpression extends BasePlanElement implements ExpressionNode
{
    private DataTypeDescriptor sqlType;
    private AkType akType;
    private ValueNode sqlSource;

    protected BaseExpression(DataTypeDescriptor sqlType, AkType akType, ValueNode sqlSource) {
        this.sqlType = sqlType;
        this.akType = akType;
        this.sqlSource = sqlSource;
    }

    protected BaseExpression(DataTypeDescriptor sqlType, ValueNode sqlSource) {
        this(sqlType, (sqlType != null) ? TypesTranslation.sqlTypeToAkType(sqlType) : AkType.UNSUPPORTED, sqlSource);
    }

    @Override
    public DataTypeDescriptor getSQLtype() {
        return sqlType;
    }

    @Override
    public AkType getAkType() {
        return akType;
    }

    @Override
    public ValueNode getSQLsource() {
        return sqlSource;
    }

    protected void setSQLtype(DataTypeDescriptor type) {
        this.sqlType = type;
    }

    @Override
    public boolean isColumn() {
        return false;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Don't clone AST or type.
    }

}
