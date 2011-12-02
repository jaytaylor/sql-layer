/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.store.IndexRecordVisitor;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * <p>Various atomic tests of GI maintenance.</p>
 *
 *<p>Each test is named foo_action_X where:
 * <ul>
 * <li>foo = the initial state of the db</li>
 * <li>action = add, del (delete), move</li>
 * <li>X = one of {@code [c a o i h]}, the type of row which is being added/deleted/moved</li>
 * </ul></p>
 *
 * The grouping is:
 * <pre>
 * c
 * |- a
 * |- o
 *    |- i
 *       |- h</pre>
 * <p>For instance, {@link #coh_add_i()} would start with a db where there's a customer with an order, and an orphaned
 * handling row; it would then add the item that links the customer-order to the handling, and check the GIs.</p>
 *
 * <p>About the PKs: each one has as many digits as its depth, and its rightmost (N-1) digits correspond to its
 * parent. For instance, an item row would have a 3-digits PK (its depth is 3), and its 2 rightmost digits would
 * refer to its parent order. This is true even if the given order doesn't exist. The leftmost digit simply increments.
 * So, the first order for cid 1 would have a PK of 11, and the next order for that customer would be 21. The first
 * order for cid 2 would have a PK of 12.</p>
 *
 * <p>Some of these tests contain multiple branches. These have names like {@link #coihCIH_move_o()}. In this case,
 * we have two branches initially: one has coih rows and the other has c, no o, and an orphaned i that has an h. It then
 * moves the o from the first branch to the second, such that the first now has an orphaned i, and the second adopts
 * and now has an unbroken branch.</p>
 */
public final class NewGiUpdateIT extends ITBase {

    private GisChecker initC() {
        writeRow(c, 117L, "John");
        return checker()
                .gi("name_when_LFET")
                .entry("John, null, 117, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void c_add_c() {
        GisChecker initState = initC();

        writeRow(c, 6L, "Noble");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 117, null").backedBy(c)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 6L);
        initState.check();
    }

    @Test
    public void c_add_o() {
        GisChecker initState = initC();

        writeRow(o, 7L, 117L, "2552-08-30");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2552-08-30, 117, 7").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 7L, 117L);
        initState.check();
    }

    @Test
    public void c_add_I() {
        GisChecker initState = initC();

        writeRow(i, 101L, 7L, "1234");
        initState.check();

        deleteRow(i, 101L, 7L);
        initState.check();
    }

    @Test
    public void c_add_h() {
        GisChecker initState = initC();

        writeRow(h, 1001L, 101L, "don't drop");
        initState.check();
        
        deleteRow(h, 1001L, 101L);
        initState.check();
    }

    @Test
    public void c_add_a() {
        GisChecker initState = initC();

        writeRow(a, 40L, 117L, "Reach");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 117, null").backedBy(c)
                .gi("street_name_RIGHT")
                .entry("Reach, John, 117, 40").backedBy(c, a)
                .done();

        deleteRow(a, 40L, 117L);
        initState.check();
    }

    @Test
    public void c_del_c() {
        initC();

        deleteRow(c, 117L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker init_cC() {
        writeRow(c, 6L, "Noble");
        writeRow(c, 117L, "John");

        return checker()
                .gi("name_when_LFET")
                .entry("John, null, 117, null").backedBy(c)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void cC_add_o() {
        GisChecker initState = init_cC();

        writeRow(o, 10L, 117L, "1970-01-01");
        checker()
                .gi("name_when_LFET")
                .entry("John, 1970-01-01, 117, 10").backedBy(c, o)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 10L, 117L);
        initState.check();
    }

    @Test
    public void cC_del_c() {
        init_cC();

        deleteRow(c, 117L);
        checker()
                .gi("name_when_LFET")
                .entry("Noble, null, 6, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker init_o() {
        writeRow(o, 11L, 1L, "2001-01-01");

        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void o_add_c() {
        GisChecker initState = init_o();

        writeRow(c, 1L, "Bob");
        checker()
                .gi("name_when_LFET")
                .entry("Bob, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void o_add_o() {
        GisChecker initState = init_o();

        writeRow(o, 21L, 1L, "2002-02-02");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void o_add_I() {
        GisChecker initState = init_o();

        writeRow(i, 111L, 11L, "1234");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void o_add_h() {
        GisChecker initState = init_o();

        writeRow(h, 1111L, 111L, "careful");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void o_add_a() {
        GisChecker initState = init_o();

        writeRow(a, 11L, 1L, "Harrison Ave");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .entry("Harrison Ave, null, 1, 11").backedBy(a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    private GisChecker init_oo() {
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(o, 21L, 1L, "2002-02-02");

        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void oo_add_c() {
        GisChecker initState = init_oo();

        writeRow(c, 1L, "John");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void oo_add_i() {
        GisChecker initState = init_oo();

        writeRow(i, 111L, 11L, "1234");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void oo_add_h() {
        GisChecker initState = init_oo();

        writeRow(h, 1111L, 111L, "careful");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void oo_del_o() {
        init_oo();

        deleteRow(o, 11L, 1L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker init_i() {
        writeRow(i, 111L, 11L, "1234");
        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void i_add_c() {
        GisChecker initState = init_i();

        writeRow(c, 1L, "John");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void i_add_o() {
        GisChecker initState = init_i();

        writeRow(o, 11L, 1L, "2001-01-01");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void i_add_i() {
        GisChecker initState = init_i();

        writeRow(i, 211L, 11L, "5678");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 211L, 11L);
        initState.check();
    }

    @Test
    public void i_add_h() {
        GisChecker initState = init_i();

        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void i_add_a() {
        GisChecker initState = init_i();

        writeRow(a, 11L, 1L, "Mass Ave");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .entry("Mass Ave, null, 1, 11").backedBy(a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    private GisChecker init_ii() {
        writeRow(i, 111L, 11L, "1234");
        writeRow(i, 211L, 11L, "5678");

        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void ii_add_c() {
        GisChecker initState = init_ii();

        writeRow(c, 1L, "John");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void ii_add_o() {
        GisChecker initState = init_ii();

        writeRow(o, 11L, 1L, "2001-01-01");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void ii_add_h() {
        init_ii();

        writeRow(h, 1111L, 111L, "dont drop!");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
    }

    @Test
    public void ii_del_i() {
        init_ii();

        deleteRow(i, 111L, 11L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker initH() {
        writeRow(h, 1111L, 111L, "don't let it break");
        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void h_add_c() {
        GisChecker initState = initH();

        writeRow(c, 1L, "John");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L, "John");
        initState.check();
    }

    @Test
    public void h_add_o() {
        GisChecker initState = initH();

        writeRow(o, 11L, 1L, "2001-01-01");
        initState.check();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void h_add_i() {
        GisChecker initState = initH();

        writeRow(i, 111L, 11L, "1234");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void h_add_h() {
        GisChecker initState = initH();

        writeRow(h, 2111L, 111L, "it's fine if it breaks");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 2111L, 111L);
        initState.check();
    }

    private GisChecker init_co() {
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(c, 1L, "John");
        return checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void co_add_o() {
        GisChecker initState = init_co();
        
        writeRow(o, 21L, 1L, "2002-02-02");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void co_add_i() {
        GisChecker initState = init_co();

        writeRow(i, 111L, 11L, "1234");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void co_add_h() {
        GisChecker initState = init_co();

        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void co_del_c() {
        init_co();

        deleteRow(c, 1L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void co_del_o() {
        init_co();

        deleteRow(o, 11L, 1L);
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker init_ci() {
        writeRow(c, 1L, "John");
        writeRow(i, 111L, 11L, "1234");
        return checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }


    @Test
    public void ci_add_o() {
        GisChecker initState = init_ci();

        writeRow(o, 11L, 1L, "2001-01-01");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void ci_add_h() {
        GisChecker initState = init_ci();

        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    private GisChecker init_oi() {
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(i, 111L, 11L, "1234");
        return checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void oi_add_c() {
        GisChecker initState = init_oi();

        writeRow(c, 1L, "John");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 1L, "John");
        initState.check();
    }

    @Test
    public void oi_add_h() {
        GisChecker initState = init_oi();

        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void oi_del_o() {
        init_oi();

        deleteRow(o, 11L, 1L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void oi_del_i() {
        init_oi();

        deleteRow(i, 111L, 11L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void ih_add_o() {
        writeRow(i, 111L, 11L, "1234");
        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();

        writeRow(o, 11L, 1L, "2001-01-01");
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    private GisChecker init_coi() {
        writeRow(c, 1L, "John");
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(i, 111L, 11L, "1234");
        return checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void coi_add_h() {
        GisChecker initState = init_coi();

        writeRow(h, 1111L, 111L, "don't drop");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void coi_add_a() {
        GisChecker initState = init_coi();

        writeRow(a, 11L, 1L, "Harrison");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .entry("Harrison, John, 1, 11").backedBy(c, a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    @Test
    public void coh_add_i() {
        writeRow(c, 1L, "John");
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(h, 1111L, 111L, "don't drop");
        GisChecker initState = checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        writeRow(i, 111L, 11L, "1234");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    private GisChecker init_coih() {
        writeRow(c, 1L, "John");
        writeRow(o, 11L, 1L, "2001-01-01");
        writeRow(i, 111L, 11L, "1234");
        writeRow(h, 1111L, 111L, "don't drop");
        return checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void coih_add_c() {
        GisChecker initState = init_coih();

        writeRow(c, 2L, "Bob");
        checker()
                .gi("name_when_LFET")
                .entry("Bob, null, 2, null").backedBy(c)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(c, 2L);
        initState.check();
    }

    @Test
    public void coih_add_o() {
        GisChecker initState = init_coih();

        writeRow(o, 21L, 1L, "2002-02-02");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void coih_add_i() {
        GisChecker initState = init_coih();

        writeRow(i, 211L, 11L, "5678");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(i, 211L, 11L);
        initState.check();
    }

    @Test
    public void coih_add_h() {
        GisChecker initState = init_coih();

        writeRow(h, 2111L, 111L, "fine if it drops");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        deleteRow(h, 2111L, 111L);
        initState.check();
    }

    @Test
    public void coih_add_a() {
        GisChecker initState = init_coih();

        writeRow(a, 11L, 1L, "Mass Ave");
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .entry("Mass Ave, John, 1, 11").backedBy(c, a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    @Test
    public void coih_del_c() {
        init_coih();

        deleteRow(c, 1L);
        checker()
                .gi("name_when_LFET")
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void coih_del_o() {
        init_coih();

        deleteRow(o, 11L, 1L);
        checker()
                .gi("name_when_LFET")
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void coih_del_i() {
        init_coih();

        deleteRow(i, 111L, 11L);
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
    }

    @Test
    public void coih_del_h() {
        init_coih();

        deleteRow(h, 1111L, 111L);
        checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
    }

    private void init_coih_COIH(int... which) {
        init_coih();
        Set<Integer> whichSet = new HashSet<Integer>();
        for (int whichInt : which)
            whichSet.add(whichInt);
        assertFalse("no COIH tables provided", whichSet.isEmpty());
        for (int whichInt : whichSet) {
            if (whichInt == c)
                writeRow(c, 2L, "Bob");
            else if (whichInt == o)
                writeRow(o, 12L, 2L, "2002-02-02");
            else if (whichInt == i)
                writeRow(i, 112L, 12L, "5678");
            else if (whichInt == h)
                writeRow(h, 1112L, 112L, "be careful");
            else
                throw new RuntimeException("unknown table id: " + whichInt);
        }
    }

    @Test
    public void coihOIH_move_c() {
        init_coih_COIH(o, i, h);
        GisChecker initState = checker()
                .gi("name_when_LFET")
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(c, 1L, "John").to(2L, "Johnny");
        checker()
                .gi("name_when_LFET")
                .entry("Johnny, 2002-02-02, 2, 12").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(c, 2L, "Johnny").to(1L, "John");
        initState.check();
    }

    @Test
    public void coihCIH_move_o() {
        init_coih_COIH(c, i, h);
        GisChecker initState = checker()
                .gi("name_when_LFET")
                .entry("Bob, null, 2, null").backedBy(c)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(o, 11L, 1L, "2001-01-1").to(12L, 2L, "1999-12-31");
        checker()
                .gi("name_when_LFET")
                .entry("Bob, 1999-12-31, 2, 12").backedBy(c, o)
                .entry("John, null, 1, null").backedBy(c)
                .gi("street_name_RIGHT")
                .done();

        update(o, 12L, 2L, "1999-12-31").to(11L, 1L, "2001-01-01");
        initState.check();
    }

    @Test
    public void coihCOH_move_I() {
        init_coih_COIH(c, o, h);
        GisChecker initState = checker()
                .gi("name_when_LFET")
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(i, 111L, 11L, "1234").to(112L, 12L, "3456");
        checker()
                .gi("name_when_LFET")
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(i, 112L, 12L, "3456").to(111L, 11L, "1234");
        initState.check();
    }

    @Test
    public void coihCOI_move_h() {
        init_coih_COIH(c, o, i);
        GisChecker initState = checker()
                .gi("name_when_LFET")
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();

        update(h, 1111L, 111L, "don't drop").to(1112L, 112L, "handle with care");
        checker()
                .gi("name_when_LFET")
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi("street_name_RIGHT")
                .done();
        
        update(h, 1112L, 112L, "handle with care").to(1111L, 111L, "don't drop");
        initState.check();
    }

    @Before
    public final void createTables() {
        c = createTable(SCHEMA, "c", "cid int key, name varchar(32)");
        o = createTable(SCHEMA, "o", "oid int key, c_id int, when varchar(32)", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int key, o_id int, sku int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "sid int key, i_id int, handling_instructions varchar(32)", akibanFK("i_id", "i", "iid") );
        a = createTable(SCHEMA, "a", "oid int key, c_id int, street varchar(56)", akibanFK("c_id", "c", "cid") );

        String groupName = group().getName();

        createGroupIndex(groupName, "name_when_LFET", "c.name,o.when", Index.JoinType.LEFT);
        createGroupIndex(groupName, "street_name_RIGHT", "a.street,c.name", Index.JoinType.RIGHT);
    }

    @After
    public final void forgetTables() throws PersistitException {
        dml().truncateTable(session(), a);
        dml().truncateTable(session(), i);
        dml().truncateTable(session(), o);
        dml().truncateTable(session(), c);

        GisCheckBuilder emptyCheckBuilder = checker();
        for (GroupIndex gi : group().getIndexes()) {
            emptyCheckBuilder.gi(gi.getIndexName().getName());
        }
        emptyCheckBuilder.done();

        c = null;
        o = null;
        i = null;
        h = null;
        a = null;
        assertEquals(Collections.<GisCheckBuilder>emptySet(), unfinishedCheckBuilders);
    }

    private GisCheckBuilder checker() {
        StackTraceElement callerFrame = Thread.currentThread().getStackTrace()[2];
        GisCheckBuilder checkBuilder = new GiCheckBuilderImpl(callerFrame);
        boolean added = unfinishedCheckBuilders.add(checkBuilder);
        assert added : unfinishedCheckBuilders;
        return checkBuilder;
    }

    private Group group() {
        return getUserTable(c).getGroup();
    }

    private Integer c;
    private Integer o;
    private Integer i;
    private Integer h;
    private Integer a;
    private final Set<GisCheckBuilder> unfinishedCheckBuilders = new HashSet<GisCheckBuilder>();

    private static final String SCHEMA = "coia";

    // nested classes

    private interface GisChecker {
        public void check();
    }

    private interface GisCheckBuilder {
        GiCheckBuilder gi(String giName);
        public GisChecker done();
    }

    private interface GiCheckBuilder extends GisCheckBuilder {
        public GiTablesChecker entry(String key);
    }

    private interface GiTablesChecker {
        GiCheckBuilder backedBy(int firstTableId, int... tableIds);
    }

    private class GiCheckBuilderImpl implements GiCheckBuilder, GiTablesChecker {

        @Override
        public GiCheckBuilder gi(String giName) {
            assertEquals("", scratch.toString());
            giToCheck = group().getIndex(giName);
            assertNotNull("no GI named " + giName, giToCheck);
            List<String> oldList = expectedStrings.put(giToCheck, new ArrayList<String>());
            assertEquals(null, oldList);
            return this;
        }

        @Override
        public GiTablesChecker entry(String key) {
            assertEquals("", scratch.toString());
            scratch.append('[').append(key).append("] => ");
            return this;
        }

        @Override
        public GiCheckBuilder backedBy(int firstTableId, int... tableIds) {
            String scratchString = scratch.toString();
            assertTrue(scratchString, scratchString.endsWith(" => "));
            assertNotNull(giToCheck);

            Set<UserTable> containingTables = new HashSet<UserTable>();
            AkibanInformationSchema ais = ddl().getAIS(session());
            containingTables.add(ais.getUserTable(firstTableId));
            for (int tableId : tableIds) {
                containingTables.add(ais.getUserTable(tableId));
            }
            long result = 0;
            int giValueIndex = 0;
            for(UserTable table = giToCheck.leafMostTable();
                table != giToCheck.rootMostTable().parentTable();
                table = table.parentTable())
            {
                if (containingTables.remove(table)) {
                    result |= 1 << giValueIndex;
                }
                ++giValueIndex;
            }
            if (!containingTables.isEmpty())
                throw new RuntimeException("tables specified not in the branch: " + containingTables);
            assert Long.bitCount(result) == tableIds.length + 1;
            String valueAsString =  Long.toBinaryString(result) + " (Long)";

            expectedStrings.get(giToCheck).add(scratch.append(valueAsString).toString());
            scratch.setLength(0);

            return this;
        }

        public GisChecker done() {
            unfinishedCheckBuilders.remove(this);
            GisChecker checker = new GisCheckerImpl(expectedStrings);
            checker.check();
            return checker;
        }

        @Override
        public String toString() {
            return String.format("%s at line %d", frame.getMethodName(), frame.getLineNumber());
        }

        private GiCheckBuilderImpl(StackTraceElement frame) {
            this.frame = frame;
            this.scratch = new StringBuilder();
            this.expectedStrings = new HashMap<GroupIndex, List<String>>();
        }

        private final StackTraceElement frame;
        private final Map<GroupIndex,List<String>> expectedStrings;
        private GroupIndex giToCheck;
        private final StringBuilder scratch;
    }

    private class GisCheckerImpl implements GisChecker {

        @Override
        public void check() {
            Collection<GroupIndex> gis = group().getIndexes();
            Set<GroupIndex> uncheckedGis = new HashSet<GroupIndex>(gis);
            if (gis.size() != uncheckedGis.size())
                fail(gis + ".size() != " + uncheckedGis + ".size()");

            for(Map.Entry<GroupIndex,List<String>> checkPair : expectedStrings.entrySet()) {
                GroupIndex gi = checkPair.getKey();
                List<String> expected = checkPair.getValue();
                checkIndex(gi, expected);
                uncheckedGis.remove(gi);
            }

            if (!uncheckedGis.isEmpty()) {
                List<String> uncheckedGiNames = new ArrayList<String>();
                for (GroupIndex gi : uncheckedGis) {
                    uncheckedGiNames.add(gi.getIndexName().getName());
                }
                fail("unchecked GIs: " + uncheckedGiNames.toString());
            }
        }

        private void checkIndex(GroupIndex groupIndex, List<String> expected) {
            final StringsIndexScanner scanner;
            try {
                scanner= persistitStore().traverse(session(), groupIndex, new StringsIndexScanner());
            } catch (PersistitException e) {
                throw new RuntimeException(e);
            }

            if (!expected.equals(scanner.strings())) {
                assertEquals("scan of " + groupIndex, Strings.join(expected), Strings.join(scanner.strings()));
                // just in case
                assertEquals("scan of " + groupIndex, expected, scanner.strings());
            }
        }

        private GisCheckerImpl(Map<GroupIndex, List<String>> expectedStrings) {
            this.expectedStrings = new HashMap<GroupIndex, List<String>>(expectedStrings);
        }

        private final Map<GroupIndex,List<String>> expectedStrings;
    }

    private static class StringsIndexScanner extends IndexRecordVisitor {

        // IndexRecordVisitor interface

        @Override
        public void visit(List<?> key, Object value) {
            final String asString;
            if (value == null) {
                asString = String.format("%s => null", key);
            }
            else {
                final String className;
                if (value instanceof Long) {
                    value = Long.toBinaryString((Long)value);
                    className = "Long";
                }
                else {
                    className = value.getClass().getSimpleName();
                }
                asString = String.format("%s => %s (%s)", key, value, className);
            }
            _strings.add(asString);
        }

        // StringsIndexScanner interface

        public List<String> strings() {
            return _strings;
        }

        // object state

        private final List<String> _strings = new ArrayList<String>();
    }
}
