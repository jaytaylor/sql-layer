
package com.akiban.sql.optimizer.plan;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

public interface ExpressionNode extends PlanElement
{
    public DataTypeDescriptor getSQLtype();
    public AkType getAkType();
    public ValueNode getSQLsource();
    public AkCollator getCollator();
    public TPreptimeValue getPreptimeValue();

    public void setPreptimeValue(TPreptimeValue value);
    public void setSQLtype(DataTypeDescriptor type);

    public boolean isColumn();
    public boolean isConstant();

    public boolean accept(ExpressionVisitor v);
    public ExpressionNode accept(ExpressionRewriteVisitor v);
}
