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

package com.akiban.qp.row;

// Row and RowHolder implement a reference-counting scheme for rows. Operators should hold onto
// Rows using a RowHolder. Assignments to RowHolder (using RowHolder.set) cause reference counting
// to be done, via Row.share() and release(). isShared() is used by data sources (a btree cursor)
// when the row filled in by the cursor is about to be changed. If isShared() is true, then the cursor
// allocates a new row and writes into it, otherwise the existing row is reused. (isShared() is true iff the reference
// count is > 1. If the reference count is = 1, then presumably the reference is from the btree cursor itself.)
//
// This implementation is NOT threadsafe. It assumes that all access to a Row is within one thread.
// E.g., when a Row is returned from an operator's next(), it is often not in a RowHolder owned by that operator,
// and the reference count could be zero. As long as we're in one thread, the cursor can't be writing new data into
// the row while this is happening.

import com.akiban.util.Shareable;

public interface Row extends RowBase, Shareable
{
}
