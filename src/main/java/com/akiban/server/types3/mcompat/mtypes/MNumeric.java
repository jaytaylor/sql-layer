package com.akiban.server.types3.mcompat.mtypes;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCombineMode;
import com.akiban.server.types3.TFactory;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.MBundle;
import com.akiban.server.types3.pvalue.PUnderlying;

public class MNumeric extends TClass {

    private MNumeric(String name, int serializationSize, PUnderlying pUnderlying) {
        super(MBundle.INSTANCE.id(), name, 
                IntAttribute.values(),
                1, 1, serializationSize, 
                pUnderlying);
    }

    @Override
    public TFactory factory() {
        return new MNumericFactory(this);
    }

    @Override
    protected TInstance doCombine(TCombineMode mode, TInstance instance0, TInstance instance1) {
        // Determine precision of TInstance
        /*switch (mode) {
            case COMBINE:
            case CHOOSE:
        }*/
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    // numeric types
    public static final TClass BIT = new MNumeric("bit", 8, PUnderlying.INT_64);
    public static final TClass TINYINT = new MNumeric("tinyint", 1, PUnderlying.INT_8);
    public static final TClass TINYINT_UNSIGNED = new MNumeric("tinyintunsigned", 4, PUnderlying.INT_16);
    public static final TClass SMALLINT = new MNumeric("smallint", 2, PUnderlying.INT_16);
    public static final TClass SMALLINT_UNSIGNED = new MNumeric("smallintunsigned", 4, PUnderlying.INT_32);
    public static final TClass MEDIUMINT = new MNumeric("mediumint", 3, PUnderlying.INT_32);
    public static final TClass MEDIUMINT_UNSIGNED = new MNumeric("mediumintunsigned", 8, PUnderlying.INT_64);
    public static final TClass INT = new MNumeric("int", 4, PUnderlying.INT_32);
    public static final TClass INT_UNSIGNED = new MNumeric("intunsigned", 8, PUnderlying.INT_64);
    public static final TClass BIGINT = new MNumeric("bigint", 8, PUnderlying.INT_64);
    public static final TClass BIGINT_UNSIGNED = new MNumeric("bigintunsigned", 8, PUnderlying.INT_64);
}