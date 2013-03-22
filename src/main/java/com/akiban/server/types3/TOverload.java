
package com.akiban.server.types3;

import com.google.common.base.Predicate;

import java.util.List;

public interface TOverload {

    String id();

    /**
     *
     * Name that the user will see/use
     */
    String displayName();

    /**
     *
     * Name(s) used internally by the parser/registry.
     *
     * Most of the times, the two names are the same, but they could be different
     * for certain functions, especially those that need "special treatment"
     *
     * This needs to be an array because we could be defining different functions
     * with the same implementation
     */
    String[] registeredNames();

    TOverloadResult resultType();
    List<TInputSet> inputSets();
    InputSetFlags exactInputs();
    int[] getPriorities();
    Predicate<List<? extends TPreptimeValue>> isCandidate();
}
