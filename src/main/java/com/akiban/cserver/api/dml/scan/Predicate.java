package com.akiban.cserver.api.dml.scan;

import java.util.EnumSet;

public interface Predicate {
    /**
     * Gets the row that defines this predicate's start. If and only if this predicate is
     * for an exact value (e.g., customer_id == 5), the returned object will be the same as
     * (by object identity) the result of {@linkplain #getEndRow()}.
     * @return the predicate's starting range
     */
    NewRow getStartRow();

    /**
     * Gets the row that defines this predicate's end. If and only if this predicate is
     * for an exact value (e.g., customer_id == 5), the returned object will be the same as
     * (by object identity) the result of {@linkplain #getStartRow()}.
     * @return the predicate's ending range
     */
    NewRow getEndRow();

    /**
     * Returns a copy of the scan flags set by this predicate. Not all available scan flags are relevant
     * to predicates; the DEEP flag, for instance, is not. Those flags will never be set.
     * @return a copy of the predicate's scan flags
     */
    EnumSet<ScanFlag> getScanFlags();
}
