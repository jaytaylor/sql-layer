
package com.akiban.server.expression;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.CharacterTypeAttributes;

public interface ExpressionType {
    /**
     * The type represented by this expression. {@code this.evaluation().eval()} returns a {@link ValueSource} whose
     * {@link ValueSource#getConversionType()} method must return the same type as returned by this method (or NULL).
     * @return the AkType this expression's runtime instance will eventually have
     */
    AkType getType();

    /**
     * The precision of the value that will be returned.  For string
     * types, this is the maximum number of characters.  For decimal
     * numbers, it is the actual precision.  For most other types, or
     * if very difficult to compute, return <code>0</code>.
     */
    int getPrecision();

    /**
     * The scale of the value that will be returned.
     * Only meaningful for decimal numeric types.
     * Others return <code>0</code>.
     */
    int getScale();

    /**
     * The character set and collation of the value or <code>null</code>.
     */
    CharacterTypeAttributes getCharacterAttributes();
}
