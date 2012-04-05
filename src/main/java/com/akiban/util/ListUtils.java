/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ListUtils {

    /**
     * Ensures that there are no more than {@code size} elements in the given {@code list}. If the list already has
     * {@code size} elements or fewer, this doesn't do anything; otherwise, it'll remove enough elements from the
     * list to ensure that {@code list.size() == size}. Either way, by the end of this invocation,
     * {@code list.size() <= size}.
     * @param list the incoming list.
     * @param size the maximum number of elements to keep in {@code list}
     * @throws IllegalArgumentException if {@code size < 0}
     * @throws NullPointerException if {@code list} is {@code null}
     * @throws UnsupportedOperationException if the list doesn't support removal
     */
    public static void truncate(List<?> list, int size) {
        ArgumentValidation.isGTE("truncate size", size, 0);

        int rowsToRemove = list.size() - size;
        if (rowsToRemove <= 0) {
            return;
        }
        ListIterator<?> iterator = list.listIterator(list.size());
        while (rowsToRemove-- > 0) {
            iterator.previous();
            iterator.remove();
        }
    }

    public static void removeDuplicates(List<?> list) {
        Set<Object> elems = new HashSet<Object>(list.size());
        for(Iterator<?> iter = list.iterator(); iter.hasNext();) {
            Object next = iter.next();
            if (!elems.add(next))
                iter.remove();
        }
    }
}
