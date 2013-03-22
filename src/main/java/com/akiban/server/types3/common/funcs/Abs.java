package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class Abs extends TScalarBase {

    protected final TClass inputType;

    public Abs(TClass returnType) {
        this.inputType = returnType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, 0);
    }

    @Override
    public String displayName() {
        return "ABS";
    }

    @Override
    public String[] registeredNames() {
        return new String[] {"absolute", "abs"};
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }
}
