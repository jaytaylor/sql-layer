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

import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.ValuesRowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public final class ImmutableRowTest {
    @Test
    public void basic() {
        ValueHolder vh1 = new ValueHolder(AkType.LONG, 1L);
        ValueHolder vh2 = new ValueHolder(AkType.VARCHAR, "right");
        Row row = new ImmutableRow(rowType(AkType.LONG, AkType.VARCHAR), Arrays.asList(vh1, vh2).iterator());
        vh1.putLong(50);
        vh2.putString("wrong");
        assertEquals("1", 1L, row.eval(0).getLong());
        assertEquals("2", "right", row.eval(1).getString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooFewInputs() {
        new ImmutableRow(rowType(AkType.LONG), Collections.<ValueSource>emptyList().iterator());
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooManyInputs() {
        new ImmutableRow(
                rowType(AkType.LONG),
                Arrays.asList(
                        new ValueHolder(AkType.LONG, 1L),
                        new ValueHolder(AkType.VARCHAR, "bonus")).iterator()
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongInputType() {
        new ImmutableRow(
                rowType(AkType.LONG),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
    }

    @Test(expected = IllegalStateException.class)
    public void tryToClear() {
        ImmutableRow row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.clear();
    }

    @Test(expected = IllegalStateException.class)
    public void tryToGetHolder() {
        ImmutableRow row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.holderAt(0);
    }

    @Test
    public void aquire() {
        Row row = new ImmutableRow(
                rowType(AkType.VARCHAR),
                Collections.singleton(new ValueHolder(AkType.VARCHAR, "1L")).iterator()
        );
        row.acquire();
        row.acquire();
        row.acquire();
        assertEquals("isShared", false, row.isShared());
    }

    private RowType rowType(AkType... types) {
        return new ValuesRowType(null, 1, types);
    }
}
