
package com.akiban.sql.optimizer.plan;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.server.types.AkType;
import com.akiban.sql.types.CharacterTypeAttributes;
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
