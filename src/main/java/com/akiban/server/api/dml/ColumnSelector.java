
package com.akiban.server.api.dml;

/**
 * Specifies a subset of a table's columns.
 */

public interface ColumnSelector
{
    boolean includesColumn(int columnPosition);
}
