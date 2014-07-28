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

package com.foundationdb.server.test.it.keyupdate;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.store.IndexRecordVisitor;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.AssertUtils;
import com.foundationdb.util.Exceptions;
import com.foundationdb.util.Strings;
import com.foundationdb.util.tap.Tap;
import com.foundationdb.util.tap.TapReport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * <li>action = add, del (delete), move, update</li>
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
 * <p>The _extra columns in each table are always initialized to the (PK value) * 1000.
 *
 * <p>Some of these tests contain multiple branches. These have names like {@code coihCIH_move_o()}. In this case,
 * we have two branches initially: one has coih rows and the other has c, no o, and an orphaned i that has an h. It then
 * moves the o from the first branch to the second, such that the first now has an orphaned i, and the second adopts
 * and now has an unbroken branch.</p>
 */
public final class NewGiUpdateIT extends ITBase {

    private GisChecker initC() {
        writeRow(c, 117L, "John", 117000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void c_add_c() {
        GisChecker initState = initC();

        writeRow(c, 6L, "Noble", 6000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 6L);
        initState.check();
    }

    @Test
    public void c_add_o() {
        GisChecker initState = initC();

        writeRow(o, 7L, 117L, "2552-08-30", 7000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2552-08-30, 117, 7").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2552-08-30, 117, 7").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 7L, 117L);
        initState.check();
    }

    @Test
    public void c_add_i() {
        GisChecker initState = initC();

        writeRow(i, 101L, 7L, "1234", 101000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 7, 101, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 101L, 7L);
        initState.check();
    }

    @Test
    public void c_add_h() {
        GisChecker initState = initC();

        writeRow(h, 1001L, 101L, "don't drop", 1001000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't drop, null, null, 101, 1001").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();
        
        deleteRow(h, 1001L, 101L);
        initState.check();
    }

    @Test
    public void c_add_a() {
        GisChecker initState = initC();

        writeRow(a, 40L, 117L, "Reach", 40000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
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
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private GisChecker init_cC() {
        writeRow(c, 6L, "Noble", 6000L);
        writeRow(c, 117L, "John", 117000L);

        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 117, null").backedBy(c)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void cC_add_o() {
        GisChecker initState = init_cC();

        writeRow(o, 10L, 117L, "1970-01-01", 10000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 1970-01-01, 117, 10").backedBy(c, o)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 1970-01-01, 117, 10").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 10L, 117L);
        initState.check();
    }

    @Test
    public void cC_del_c() {
        init_cC();

        deleteRow(c, 117L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Noble, null, 6, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private GisChecker init_o() {
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);

        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void o_add_c() {
        GisChecker initState = init_o();

        writeRow(c, 1L, "Bob", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void o_add_o() {
        GisChecker initState = init_o();

        writeRow(o, 21L, 1L, "2002-02-02", 21000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .entry("null, 2002-02-02, 1, 21").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void o_add_i() {
        GisChecker initState = init_o();

        writeRow(i, 111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void o_add_h() {
        GisChecker initState = init_o();

        writeRow(h, 1111L, 111L, "careful", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, careful, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void o_add_a() {
        GisChecker initState = init_o();

        writeRow(a, 11L, 1L, "Harrison Ave", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .entry("Harrison Ave, null, 1, 11").backedBy(a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    private GisChecker init_oo() {
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(o, 21L, 1L, "2002-02-02", 21000L);

        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .entry("null, 2002-02-02, 1, 21").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void oo_add_c() {
        GisChecker initState = init_oo();

        writeRow(c, 1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void oo_add_i() {
        GisChecker initState = init_oo();

        writeRow(i, 111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .entry("null, 2002-02-02, 1, 21").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void oo_add_h() {
        GisChecker initState = init_oo();

        writeRow(h, 1111L, 111L, "careful", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .entry("null, 2002-02-02, 1, 21").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, careful, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void oo_del_o() {
        init_oo();

        deleteRow(o, 11L, 1L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2002-02-02, 1, 21").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private GisChecker init_i() {
        writeRow(i, 111L, 11L, "1234", 111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void i_add_c() {
        GisChecker initState = init_i();

        writeRow(c, 1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void i_add_o() {
        GisChecker initState = init_i();

        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void i_add_i() {
        GisChecker initState = init_i();

        writeRow(i, 211L, 11L, "5678", 211000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .entry("5678, null, null, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 211L, 11L);
        initState.check();
    }

    @Test
    public void i_add_h() {
        GisChecker initState = init_i();

        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void i_add_a() {
        GisChecker initState = init_i();

        writeRow(a, 11L, 1L, "Mass Ave", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .entry("Mass Ave, null, 1, 11").backedBy(a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    private GisChecker init_ii() {
        writeRow(i, 111L, 11L, "1234", 111000L);
        writeRow(i, 211L, 11L, "5678", 211000L);

        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .entry("5678, null, null, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void ii_add_c() {
        GisChecker initState = init_ii();

        writeRow(c, 1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .entry("5678, null, null, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L);
        initState.check();
    }

    @Test
    public void ii_add_o() {
        GisChecker initState = init_ii();

        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .entry("5678, null, 1, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void ii_add_h() {
        GisChecker initState = init_ii();

        writeRow(h, 1111L, 111L, "don't drop!", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop!, null, 11, 111, 1111").backedBy(i, h)
                .entry("5678, null, null, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop!, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void ii_del_i() {
        init_ii();

        deleteRow(i, 111L, 11L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("5678, null, null, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private GisChecker initH() {
        writeRow(h, 1111L, 111L, "don't let it break", 1111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't let it break, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void h_add_c() {
        GisChecker initState = initH();

        writeRow(c, 1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't let it break, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L, "John");
        initState.check();
    }

    @Test
    public void h_add_o() {
        GisChecker initState = initH();

        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't let it break, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void h_add_i() {
        GisChecker initState = initH();

        writeRow(i, 111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't let it break, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't let it break, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void h_add_h() {
        GisChecker initState = initH();

        writeRow(h, 2111L, 111L, "it's fine if it breaks", 2111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't let it break, null, null, 111, 1111").backedBy(h)
                .entry("null, it's fine if it breaks, null, null, 111, 2111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 2111L, 111L);
        initState.check();
    }

    private GisChecker init_co() {
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(c, 1L, "John", 1000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void co_add_o() {
        GisChecker initState = init_co();
        
        writeRow(o, 21L, 1L, "2002-02-02", 21000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void co_add_i() {
        GisChecker initState = init_co();

        writeRow(i, 111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    @Test
    public void co_add_h() {
        GisChecker initState = init_co();

        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't drop, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void co_del_c() {
        init_co();

        deleteRow(c, 1L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void co_del_o() {
        init_co();

        deleteRow(o, 11L, 1L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private GisChecker init_ci() {
        writeRow(c, 1L, "John", 1000L);
        writeRow(i, 111L, 11L, "1234", 111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }


    @Test
    public void ci_add_o() {
        GisChecker initState = init_ci();

        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    @Test
    public void ci_add_h() {
        GisChecker initState = init_ci();

        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    private GisChecker init_oi() {
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(i, 111L, 11L, "1234", 111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void oi_add_c() {
        GisChecker initState = init_oi();

        writeRow(c, 1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 1L, "John");
        initState.check();
    }

    @Test
    public void oi_add_h() {
        GisChecker initState = init_oi();

        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void oi_del_o() {
        init_oi();

        deleteRow(o, 11L, 1L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, null, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void oi_del_i() {
        init_oi();

        deleteRow(i, 111L, 11L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void ih_add_o() {
        writeRow(i, 111L, 11L, "1234", 111000L);
        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        GisChecker initState = checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 11L, 1L);
        initState.check();
    }

    private GisChecker init_coi() {
        writeRow(c, 1L, "John", 1000L);
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(i, 111L, 11L, "1234", 111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void coi_add_h() {
        GisChecker initState = init_coi();

        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 1111L, 111L);
        initState.check();
    }

    @Test
    public void coi_add_a() {
        GisChecker initState = init_coi();

        writeRow(a, 11L, 1L, "Harrison", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .entry("Harrison, John, 1, 11").backedBy(c, a)
                .done();

        deleteRow(a, 11L, 1L);
        initState.check();
    }

    @Test
    public void coh_add_i() {
        writeRow(c, 1L, "John", 1000L);
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(h, 1111L, 111L, "don't drop!", 1111000L);
        GisChecker initState = checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't drop!, null, null, 111, 1111").backedBy(h)
                .gi(___RIGHT_street_name________)
                .done();

        writeRow(i, 111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop!, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop!, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 111L, 11L);
        initState.check();
    }

    private GisChecker init_coih() {
        writeRow(c, 1L, "John", 1000L);
        writeRow(o, 11L, 1L, "2001-01-01", 11000L);
        writeRow(i, 111L, 11L, "1234", 111000L);
        writeRow(h, 1111L, 111L, "don't drop", 1111000L);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void coih_add_c() {
        GisChecker initState = init_coih();

        writeRow(c, 2L, "Bob", 2000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, null, 2, null").backedBy(c)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(c, 2L);
        initState.check();
    }

    @Test
    public void coih_add_o() {
        GisChecker initState = init_coih();

        writeRow(o, 21L, 1L, "2002-02-02", 21000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .entry("John, 2002-02-02, 1, 21").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(o, 21L, 1L);
        initState.check();
    }

    @Test
    public void coih_add_i() {
        GisChecker initState = init_coih();

        writeRow(i, 211L, 11L, "5678", 211000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, null, 1, 11, 211, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(i, 211L, 11L);
        initState.check();
    }

    @Test
    public void coih_add_h() {
        GisChecker initState = init_coih();

        writeRow(h, 2111L, 111L, "fine if it drops", 2111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("1234, fine if it drops, 1, 11, 111, 2111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("1234, fine if it drops, 1, 11, 111, 2111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        deleteRow(h, 2111L, 111L);
        initState.check();
    }

    @Test
    public void coih_add_a() {
        GisChecker initState = init_coih();

        writeRow(a, 11L, 1L, "Mass Ave", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
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
                .gi(___LEFT_name_when___________)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .gi(___RIGHT_street_name________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .done();
    }

    @Test
    public void coih_del_o() {
        init_coih();

        deleteRow(o, 11L, 1L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .gi(___RIGHT_street_name________)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .done();
    }

    @Test
    public void coih_del_i() {
        init_coih();

        deleteRow(i, 111L, 11L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_street_name________)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't drop, null, null, 111, 1111").backedBy(h)
                .done();
    }

    @Test
    public void coih_del_h() {
        init_coih();

        deleteRow(h, 1111L, 111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .done();
    }

    private void init_coih_COIH(int... which) {
        init_coih();
        Set<Integer> whichSet = new HashSet<>();
        for (int whichInt : which)
            whichSet.add(whichInt);
        assertFalse("no COIH tables provided", whichSet.isEmpty());
        for (int whichInt : whichSet) {
            if (whichInt == c)
                writeRow(c, 2L, "Bob", 2000L);
            else if (whichInt == o)
                writeRow(o, 12L, 2L, "2002-02-02", 12000L);
            else if (whichInt == i)
                writeRow(i, 112L, 12L, "5678", 112000L);
            else if (whichInt == h)
                writeRow(h, 1112L, 112L, "be careful", 1112000L);
            else
                throw new RuntimeException("unknown table id: " + whichInt);
        }
    }

    @Test
    public void coihOIH_move_c() {
        init_coih_COIH(o, i, h);
        GisChecker initState = checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2002-02-02, 2, 12").backedBy(o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(c, 1L, "John", 1000L).to(2L, "Johnny", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Johnny, 2002-02-02, 2, 12").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("null, 2001-01-01, 1, 11").backedBy(o)
                .entry("Johnny, 2002-02-02, 2, 12").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(c, 2L, "Johnny", 1000L).to(1L, "John", 1000L);
        initState.check();
    }

    private GisChecker init_coihCIH() {
        init_coih_COIH(c, i, h);
        return checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, null, 2, null").backedBy(c)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();
    }

    @Test
    public void coihCIH_move_o_changeCid() {
        GisChecker initState = init_coihCIH();

        update(o, 11L, 1L, "2001-01-1", 11000L).to(21L, 1L, "1999-12-31", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, null, 2, null").backedBy(c)
                .entry("John, 1999-12-31, 1, 21").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 1999-12-31, 1, 21").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(o, 21L, 1L, "1999-12-31", 11000L).to(11L, 1L, "2001-01-01", 11000L);
        initState.check();
    }

    @Test
    public void coihCIH_move_o_changeOid() {
        GisChecker initState = init_coihCIH();

        // This one's a bit tricky, because the pks/fks don't match up anymore in terms of our naming conventions.
        // Once the update is done, oid=11 now belongs to cid=2, but it doesn't change its children items. That means
        // the items' PK have to be adjusted, but only at the cid level.
        //
        // Before:
        // c   1               1 John
        // o   1,11            11 1 2001-01-01
        // i   1,11,111        111 11 1234
        // h   1,11,111,1111   1111 111 don't drop
        // c   2               2 Bob
        // i   2,12,112        112 12 5678
        // h   2,12,112,1112   1112 112 be careful
        //
        // After:
        // c   1               1 John
        // c   2               2 Bob
        // o   2,11            11 1 2001-01-01
        // i   2,11,111        111 11 1234
        // h   2,11,111,1111   1111 111 don't drop
        // i   _,12,112        112 12 5678
        // h   _,12,112,1112   1112 112 be careful

        update(o, 11L, 1L, "2001-01-1", 11000L).to(11L, 2L, "1999-12-31", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 1999-12-31, 2, 11").backedBy(c, o)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 2, 11, 111, 1111").backedBy(i, h)    // Note! oid=11 still belongs to iid=112
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h) // Note! oid=11 doesn't adopt iid=112
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 1999-12-31, 2, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 2, 11, 111, 1111").backedBy(i, h)    // these two are like the left joins
                .entry("5678, be careful, null, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(o, 11L, 2L, "1999-12-31", 11000L).to(11L, 1L, "2001-01-01", 11000L);
        initState.check();
    }

    @Test
    public void coihCIH_move_o_changeBoth() {
        GisChecker initState = init_coihCIH();

        update(o, 11L, 1L, "2001-01-1", 11000L).to(12L, 2L, "1999-12-31", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 1999-12-31, 2, 12").backedBy(c, o)
                .entry("John, null, 1, null").backedBy(c)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 1999-12-31, 2, 12").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, null, 11, 111, 1111").backedBy(i, h)
                .entry("5678, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(o, 12L, 2L, "1999-12-31", 11000L).to(11L, 1L, "2001-01-01", 11000L);
        initState.check();
    }

    @Test
    public void coihCOH_move_i() {
        init_coih_COIH(c, o, h);
        GisChecker initState = checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, be careful, null, null, 112, 1112").backedBy(h)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(i, 111L, 11L, "1234", 111000L).to(112L, 12L, "3456", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("3456, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("null, don't drop, null, null, 111, 1111").backedBy(h)
                .entry("3456, be careful, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(i, 112L, 12L, "3456", 111000L).to(111L, 11L, "1234", 111000L);
        initState.check();
    }

    @Test
    public void coihCOI_move_h() {
        init_coih_COIH(c, o, i);
        GisChecker initState = checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .entry("5678, null, 2, 12, 112, null").backedBy(i)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();

        update(h, 1111L, 111L, "don't drop").to(1112L, 112L, "handle with care");
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, null, 1, 11, 111, null").backedBy(i)
                .entry("5678, handle with care, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("Bob, 2002-02-02, 2, 12").backedBy(c, o)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("5678, handle with care, 2, 12, 112, 1112").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .done();
        
        update(h, 1112L, 112L, "handle with care").to(1111L, 111L, "don't drop");
        initState.check();
    }

    // Tests of GI maintenance optimization -- avoiding maintenance when unaffected columns are updated.

    @Test
    public void update_c_skip_not_possible_1()
    {
        init_co();
        // Update name, which is involved in all indexes on customer.
        update(c, 1L, "John", 1000L).to(1L, "Paul", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Paul, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("Paul, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_c_skip_not_possible_2()
    {
        init_co();
        // Updating both name and extra -- same number of GI maintenance actions as for updating name.
        update(c, 1L, "John", 1000L).to(1L, "Paul", 1111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("Paul, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("Paul, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_c_skip_no_actual_update()
    {
        init_co();
        // If there is no actual update, all maintenance (twice for each index involving customer) should be skipped
        update(c, 1L, "John", 1000L).to(1L, "John", 1000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(6)
                .done();
    }

    @Test
    public void update_c_skip_update_unindexed()
    {
        init_co();
        // There are 3 indexes involving the customer table and each may be updated twice.
        // If extra is updated, which is not involved in any index, then the expected number
        // of GI maintenance actions skipped is 6.
        update(c, 1L, "John", 1000L).to(1L, "John", 1111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(6)
                .done();
    }

    @Test
    public void update_o_skip_not_possible_1()
    {
        init_co();
        // Update when, which is involved in all indexes on order.
        update(o, 11L, 1L, "2001-01-01", 11000L).to(11L, 1L, "2011-11-11", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2011-11-11, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2011-11-11, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_o_skip_not_possible_2()
    {
        init_co();
        // Updating both when and extra -- same number of GI maintenance actions as for updating when.
        update(o, 11L, 1L, "2001-01-01", 11000L).to(11L, 1L, "2011-11-11", 11111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2011-11-11, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2011-11-11, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_o_skip_no_actual_update()
    {
        init_co();
        // If there is no actual update, all maintenance (twice for each index involving order) should be skipped
        update(o, 11L, 1L, "2001-01-01", 11000L).to(11L, 1L, "2001-01-01", 11000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Test
    public void update_o_skip_update_unindexed()
    {
        init_co();
        // There are 2 indexes involving the order table and each may be updated twice.
        // If extra is updated, which is not involved in any index, then the expected number
        // of GI maintenance actions skipped is 4.
        update(o, 11L, 1L, "2001-01-01", 11000L).to(11L, 1L, "2001-01-01", 11111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Test
    public void update_i_skip_not_possible_1()
    {
        init_coih();
        // Update sku, which is involved in all indexes on item.
        update(i, 111L, 11L, "1234", 111000L).to(111L, 11L, "9999", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("9999, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("9999, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_i_skip_not_possible_2()
    {
        init_coih();
        // Updating both sku and extra -- same number of GI maintenance actions as for updating sku.
        update(i, 111L, 11L, "1234", 111000L).to(111L, 11L, "9999", 111111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("9999, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("9999, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_i_skip_no_actual_update()
    {
        init_coih();
        // If there is no actual update, all maintenance (twice for each index involving order) should be skipped
        update(i, 111L, 11L, "1234", 111000L).to(111L, 11L, "1234", 111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Test
    public void update_i_skip_update_unindexed()
    {
        init_coih();
        // There are 2 indexes involving the item table and each may be updated twice.
        // If extra is updated, which is not involved in any index, then the expected number
        // of GI maintenance actions skipped is 4.
        update(i, 111L, 11L, "1234", 111000L).to(111L, 11L, "1234", 111111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Test
    public void update_h_skip_not_possible_1()
    {
        init_coih();
        // Update handling_instructions, which is involved in all indexes on item.
        update(h, 1111L, 111L, "don't drop", 1111000L).to(1111L, 111L, "lemon drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, lemon drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, lemon drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_h_skip_not_possible_2()
    {
        init_coih();
        // Updating both handling instructions and extra -- same number of GI maintenance actions as for updating
        // handling instructions.
        update(h, 1111L, 111L, "don't drop", 1111000L).to(1111L, 111L, "lemon drop", 1111111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, lemon drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, lemon drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(0)
                .done();
    }

    @Test
    public void update_h_skip_no_actual_update()
    {
        init_coih();
        // If there is no actual update, all maintenance (twice for each index involving order) should be skipped
        update(h, 1111L, 111L, "don't drop", 1111000L).to(1111L, 111L, "don't drop", 1111000L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Test
    public void update_h_skip_update_unindexed()
    {
        init_coih();
        // There are 2 indexes involving the handling table and each may be updated twice.
        // If extra is updated, which is not involved in any index, then the expected number
        // of GI maintenance actions skipped is 4.
        update(h, 1111L, 111L, "don't drop", 1111000L).to(1111L, 111L, "don't drop", 1111111L);
        checker()
                .gi(___LEFT_name_when___________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___LEFT_sku_instructions____)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_name_when__________)
                .entry("John, 2001-01-01, 1, 11").backedBy(c, o)
                .gi(___RIGHT_sku_instructions___)
                .entry("1234, don't drop, 1, 11, 111, 1111").backedBy(i, h)
                .gi(___RIGHT_street_name________)
                .checkMaintenanceSkips(4)
                .done();
    }

    @Rule
    public TestName name = new TestName();

    @BeforeClass
    public static void tapsDefaultToOn() {
        Tap.defaultToOn(true);
    }
    
    @AfterClass
    public static void tapsDefaultToOff() {
        Tap.defaultToOn(false);
    }

    @Before
    public final void createTables() {
        c = createTable(SCHEMA, "c", "cid int not null primary key, name varchar(32), c_extra int");
        o = createTable(SCHEMA, "o", "oid int not null primary key, c_id int, when varchar(32), o_extra int", akibanFK("c_id", "c", "cid") );
        i = createTable(SCHEMA, "i", "iid int not null primary key, o_id int, sku int, i_extra int", akibanFK("o_id", "o", "oid") );
        h = createTable(SCHEMA, "h", "hid int not null primary key, i_id int, handling_instructions varchar(32), s_extra int", akibanFK("i_id", "i", "iid") );
        a = createTable(SCHEMA, "a", "aid int not null primary key, c_id int, street varchar(56), a_extra int", akibanFK("c_id", "c", "cid") );

        TableName groupName = group().getName();
        Tap.setEnabled(TAP_PATTERN, false);
        createLeftGroupIndex(groupName, ___LEFT_name_when___________, "c.name", "o.when");
        createLeftGroupIndex(groupName, ___LEFT_sku_instructions____, "i.sku", "h.handling_instructions");
        createRightGroupIndex(groupName, ___RIGHT_name_when__________, "c.name", "o.when");
        createRightGroupIndex(groupName, ___RIGHT_sku_instructions___, "i.sku", "h.handling_instructions");
        createRightGroupIndex(groupName, ___RIGHT_street_name________, "a.street", "c.name");
        Tap.setEnabled(TAP_PATTERN, true);
        Tap.reset(TAP_PATTERN);
    }
    
    @After
    public final void forgetTables() {
        if (needTapHeaders) {
            log(TAP_HEADER
                    + "name\t"
                    + Strings.join(reportsByName().keySet(), "\t")
            );
            needTapHeaders = false;
        }
        log(TAP_HEADER
                + name.getMethodName() + "\t"
                + Strings.join(reportsByName().values(), "\t")
        );

        Tap.setEnabled(TAP_PATTERN, false);

        int[] ids = { a, h, i, o, c};
        int idIndex = 0;
        for(int i = 5; i >= 0; --i) {
            try {
                while(idIndex < ids.length) {
                    dml().truncateTable(session(), ids[idIndex]);
                    ++idIndex;
                }
                break;
            } catch(Exception e) {
                if(!Exceptions.isRollbackException(e) || i == 0) {
                    throw e;
                }
            }
        }

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

    private static Map<String, Long> reportsByName() {
        TapReport[] reports = Tap.getReport(TAP_PATTERN);
        Map<String,Long> reportsByName = new TreeMap<>();
        Pattern pattern = Pattern.compile(TAP_PATTERN);
        for (TapReport report : reports) {
            Matcher matcher = pattern.matcher(report.getName());
            if (!matcher.matches())
                throw new RuntimeException("pattern not matched: " + report.getName());
            String matched = matcher.group(1);
            if (matched == null) {
                matched = matcher.group(2);
            }
            assert matched != null;
            reportsByName.put(matched + " in", report.getInCount());
            reportsByName.put(matched + " out", report.getOutCount());
        }
        return reportsByName;
    }

    private static void log(String string) {
        log.debug(string);
    }

    private GisCheckBuilder checker() {
        StackTraceElement callerFrame = Thread.currentThread().getStackTrace()[2];
        GisCheckBuilder checkBuilder = new GiCheckBuilderImpl(callerFrame);
        boolean added = unfinishedCheckBuilders.add(checkBuilder);
        assert added : unfinishedCheckBuilders;
        return checkBuilder;
    }

    private Group group() {
        return getTable(c).getGroup();
    }

    private Integer c;
    private Integer o;
    private Integer i;
    private Integer h;
    private Integer a;
    private final Set<GisCheckBuilder> unfinishedCheckBuilders = new HashSet<>();

    private static final String SCHEMA = "coia";
    private static final String ___LEFT_name_when___________ = "name_when_LEFT";
    private static final String ___LEFT_sku_instructions____ = "sku_instructions_LEFT";
    private static final String ___RIGHT_name_when__________ = "name_when_RIGHT";
    private static final String ___RIGHT_sku_instructions___ = "sku_instructions_RIGHT";
    private static final String ___RIGHT_street_name________ = "street_name_RIGHT";
    private static final String TAP_PATTERN = "GI maintenance: (.+)|.+(skip_gi_maintenance)";
    private static final String TAP_HEADER = "GI TAPS:\t";
    private static boolean needTapHeaders = true;
    private static final Logger log = LoggerFactory.getLogger(NewGiUpdateIT.class);

    // nested classes

    private interface GisChecker {
        public void check();
    }

    private interface GisCheckBuilder {
        GiEntryChecker gi(String giName);
        public GisCheckBuilder checkMaintenanceSkips(int skipMaintenance);
        public GisChecker done();
    }

    private interface GiEntryChecker extends GisCheckBuilder {
        public GiTablesChecker entry(String key);
    }

    private interface GiTablesChecker {
        GiEntryChecker backedBy(int firstTableId, int... tableIds);
    }

    private class GiCheckBuilderImpl implements GiEntryChecker, GiTablesChecker {

        @Override
        public GiEntryChecker gi(String giName) {
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
        public GiEntryChecker backedBy(int firstTableId, int... tableIds) {
            String scratchString = scratch.toString();
            assertTrue(scratchString, scratchString.endsWith(" => "));
            assertNotNull(giToCheck);

            Set<Table> containingTables = new HashSet<>();
            AkibanInformationSchema ais = ddl().getAIS(session());
            containingTables.add(ais.getTable(firstTableId));
            for (int tableId : tableIds) {
                containingTables.add(ais.getTable(tableId));
            }
            long result = 0;
            for(Table table = giToCheck.leafMostTable();
                table != giToCheck.rootMostTable().getParentTable();
                table = table.getParentTable())
            {
                if (containingTables.remove(table)) {
                    result |= 1 << table.getDepth();
                }
            }
            if (!containingTables.isEmpty())
                throw new RuntimeException("tables specified not in the branch: " + containingTables);
            assert Long.bitCount(result) == tableIds.length + 1;
            String valueAsString =  Long.toBinaryString(result) + " (Long)";

            expectedStrings.get(giToCheck).add(scratch.append(valueAsString).toString());
            scratch.setLength(0);

            return this;
        }

        @Override
        public GisCheckBuilder checkMaintenanceSkips(int expectedSkipMaintenance) {
            Map<String, Long> reports = reportsByName();
            long actualSkipMaintenance = reports.get("skip_gi_maintenance in");
            assertEquals(expectedSkipMaintenance, actualSkipMaintenance);
            return this;
        }

        @Override
        public GisChecker done() {
            unfinishedCheckBuilders.remove(this);
            GisChecker checker = new GisCheckerImpl(expectedStrings);
            checker.check();
            Tap.reset(TAP_PATTERN);
            Map<String, Long> reports = reportsByName();
            return checker;
        }

        @Override
        public String toString() {
            return String.format("%s at line %d", frame.getMethodName(), frame.getLineNumber());
        }

        private GiCheckBuilderImpl(StackTraceElement frame) {
            this.frame = frame;
            this.scratch = new StringBuilder();
            this.expectedStrings = new HashMap<>();
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
            Set<GroupIndex> uncheckedGis = new HashSet<>(gis);
            if (gis.size() != uncheckedGis.size())
                fail(gis + ".size() != " + uncheckedGis + ".size()");

            for(Map.Entry<GroupIndex,List<String>> checkPair : expectedStrings.entrySet()) {
                GroupIndex gi = checkPair.getKey();
                List<String> expected = checkPair.getValue();
                checkIndex(gi, expected);
                uncheckedGis.remove(gi);
            }

            if (!uncheckedGis.isEmpty()) {
                List<String> uncheckedGiNames = new ArrayList<>();
                for (GroupIndex gi : uncheckedGis) {
                    uncheckedGiNames.add(gi.getIndexName().getName());
                }
                throw new RuntimeException("unchecked GIs: " + uncheckedGiNames.toString());
            }
        }

        private void checkIndex(final GroupIndex groupIndex, final List<String> expected) {
            transactionallyUnchecked(new Runnable() {
                @Override
                public void run() {
                    StringsIndexScanner scanner = store().traverse(session(), groupIndex, new StringsIndexScanner(), -1, 0);
                    AssertUtils.assertCollectionEquals(
                        "scan of " + groupIndex.getIndexName().getName(),
                        expected,
                        scanner.strings()
                    );
                }
            });
        }

        private GisCheckerImpl(Map<GroupIndex, List<String>> expectedStrings) {
            this.expectedStrings = new HashMap<>(expectedStrings);
        }

        private final Map<GroupIndex,List<String>> expectedStrings;
    }

    private static class StringsIndexScanner extends IndexRecordVisitor {

        // IndexVisitor interface

        @Override
        public boolean groupIndex()
        {
            return true;
        }


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

        private final List<String> _strings = new ArrayList<>();
    }
}
