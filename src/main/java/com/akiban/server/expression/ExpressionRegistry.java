
package com.akiban.server.expression;

import com.akiban.server.error.NoSuchFunctionException;

public interface ExpressionRegistry {
    ExpressionComposer composer(String name);

    public static final ExpressionRegistry EMPTY = new ExpressionRegistry() {
        @Override
        public ExpressionComposer composer(String name) {
            throw new NoSuchFunctionException(name);
        }
    };
}
