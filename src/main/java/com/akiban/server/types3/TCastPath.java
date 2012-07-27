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

package com.akiban.server.types3;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A very thin shim around List<TClass></TClass>. Mostly there so that the call sites don't have to worry about
 * generics. This is especially useful for the reflective registration, where it's easier to search for a TCastPath
 * than fora {@code Collection&lt;? extends List&lt;? extends TClass&gt;&gt;}.
 */
public final class TCastPath {

    public static TCastPath create(TClass first, TClass second, TClass third, TClass... rest) {
        return new TCastPath(first, second, third, rest);
    }

    private TCastPath(TClass first, TClass second, TClass third, TClass[] rest) {
        TClass[] all = new TClass[rest.length + 3];
        all[0] = first;
        all[1] = second;
        all[2] = third;
        System.arraycopy(rest, 0, all, 3, rest.length);
        list = Collections.unmodifiableList(Arrays.asList(all));
    }

    public List<? extends TClass> getPath() {
        return list;
    }

    private final List<? extends TClass> list;
}
