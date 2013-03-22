
package com.akiban.qp.exec;

public interface UpdateResult {
    /**
     * <p>The number of rows that were touched by this query, including those which were not modified.</p>
     *
     * <p>For instance, if you had {@code UPDATE my_table SET name='Robert'}, this would be the total number of
     * rows in {@code my_table}.</p>
     * @return the number of rows touched by the query, including for read-only scanning
     */
    int rowsTouched();

    /**
     * <p>The number of rows that were modified or deleted by this query.</p>
     *
     * <p>For instance, if you had {@code UPDATE my_table SET name='Robert'}, this would be the total number of
     * rows in {@code my_table} which did not originally have {@code name='Robert'} (and which therefore had to
     * be updated).</p>
     * @return the number of rows touched by the query, including for read-only scanning
     */
    int rowsModified();
}
