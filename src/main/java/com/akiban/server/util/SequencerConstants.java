
package com.akiban.server.util;

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
