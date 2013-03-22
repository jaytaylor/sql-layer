
package com.akiban.sql.optimizer.plan;

public abstract class AnnotatedExpression extends BaseDuplicatable
{
    private ExpressionNode expression;

    protected AnnotatedExpression(ExpressionNode expression) {
        this.expression = expression;
    }
    
    public ExpressionNode getExpression() {
        return expression;
    }
    public void setExpression(ExpressionNode expression) {
        this.expression = expression;
    }

    public boolean accept(ExpressionVisitor v) {
        return expression.accept(v);
    }

    public void accept(ExpressionRewriteVisitor v) {
        expression = expression.accept(v);
    }

    @Override
    public String toString() {
        return expression.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        expression = (ExpressionNode)expression.duplicate(map);
    }

}
