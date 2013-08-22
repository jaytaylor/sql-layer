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

package com.foundationdb.server.expression.std;

public interface Matcher
{
    boolean sameState(String pattern, char escape);

    /**
     * 
     * @param str
     * @param count
     * @return
     *      <p> a negative value if the pattern is not in <code>str</code></p>
     *      <p> a positive number indicating the index at which the pattern/substring is found</p>
     * 
     * Note: Dependent upon the implementation, it's not guaranteed that 
     * the positive number returned by this function is always the index position.
     * The positive value could simply be used as an indication that a match has been found
     */
    int match(String str, int count);
}
