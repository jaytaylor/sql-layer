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

package com.foundationdb.ais.model.aisb2;

public interface NewAkibanJoinBuilder extends NewTableBuilder {
    /**
     * Adds a child -&gt; parent column pair to this join.
     *
     * <p>{@linkplain #and(String, String)} is a synonym of this method.</p>
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder on(String childColumn, String parentColumn);

    /**
     * Synonym for {@link #on(String, String)}. This method is just here to make the code more "English-sounding."
     * Example: {@code build.joinTo("parent").on("child_col_1", "parent_col_1").and("child_col_2", "parent_col_2").}
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder and(String childColumn, String parentColumn);
}
