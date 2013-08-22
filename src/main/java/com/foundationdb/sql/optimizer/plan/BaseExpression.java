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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** An evaluated value. 
 * Usually part of a larger expression tree.
*/
public abstract class BaseExpression extends BasePlanElement implements ExpressionNode
{
    private DataTypeDescriptor sqlType;
    private AkType akType;
    private ValueNode sqlSource;
    private TPreptimeValue preptimeValue;

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

    @Override
    public void setSQLtype(DataTypeDescriptor type) {
        this.sqlType = type;
    }

    @Override
    public AkCollator getCollator() {
        if (sqlType != null) {
            CharacterTypeAttributes att = sqlType.getCharacterAttributes();
            if (att != null) {
                String coll = att.getCollation();
                if (coll != null)
                    return AkCollatorFactory.getAkCollator(coll);
            }
        }
        return null;
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

    @Override
    public TPreptimeValue getPreptimeValue() {
        return preptimeValue;
    }

    @Override
    public void setPreptimeValue(TPreptimeValue value) {
        this.preptimeValue = value;
    }
}
