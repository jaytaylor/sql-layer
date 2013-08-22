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

package com.foundationdb.server.util;

import static com.persistit.util.ThreadSequencer.allocate;
import static com.persistit.util.ThreadSequencer.array;

/**
 * COPIED FROM com.persistit.util.SequencerConstants
 *
 * Constants used in the implementation of internal tests on
 * concurrent behavior.
 * @author peter
 *
 */
public interface SequencerConstants
{
    /*
     * Used in testing a race condition between Checkpoint and Transaction commit, bug 937877
     */
    int BEFORE_GET_CONTEXT_THREAD_1 = allocate("BEFORE_GET_CONTEXT_THREAD_1");
    int BEFORE_GET_CONTEXT_THREAD_2 = allocate("BEFORE_GET_CONTEXT_THREAD_2");
    int AFTER_GET_CONTEXT_THREAD_1 = allocate("AFTER_GET_CONTEXT_THREAD_1");

    int[][] UPDATE_GET_CONTEXT_SCHEDULE = new int[][] {
            array(BEFORE_GET_CONTEXT_THREAD_1, BEFORE_GET_CONTEXT_THREAD_2), array(BEFORE_GET_CONTEXT_THREAD_1),
            array(AFTER_GET_CONTEXT_THREAD_1, BEFORE_GET_CONTEXT_THREAD_2), array(BEFORE_GET_CONTEXT_THREAD_2),
    };
}
