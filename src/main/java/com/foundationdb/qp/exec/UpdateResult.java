/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.exec;

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
