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

package com.akiban.server.service.session;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public final class SessionTest {

    private Session session;

    @Before
    public void setUp() {
        session = new Session(null);
    }

    @After
    public void tearDown() {
        session = null;
    }

    @Test
    public void keyOwner() {
        assertEquals("owner", SessionTest.class, Session.Key.<Object>named("whatever").getOwner());
    }

    @Test
    public void noDefault() {
        Session.Key<Integer> key = Session.Key.named("foo");

        Integer old1 = session.put(key, 2);
        assertNull("not null: " + old1, old1);

        Integer old2 = session.put(key, 3);
        assertEquals("old value", 2, old2.intValue());

        assertEquals("current value", 3, session.get(key).intValue());

        assertEquals("current value", 3, session.remove(key).intValue());
        Integer last = session.get(key);
        assertNull("not null: " + last, last);
    }

    @Test
    public void keyMapOwner() {
        assertEquals("owner", SessionTest.class, Session.MapKey.<Object,Object>mapNamed("whatever").getOwner());
    }

    @Test
    public void mapMethods() {
        Session.MapKey<Integer,String> key = Session.MapKey.mapNamed("foo");

        assertNull("initial value for 1", session.get(key, 1));
        assertNull("initial displacement for 1", session.put(key, 1, "one"));

        assertEquals("value for 1", "one", session.get(key, 1));
        assertEquals("removal value for 1", "one", session.remove(key, 1));

        assertNull("end value for 1", session.get(key, 1));
    }

    @Test
    public void removeFromMapBeforeGetting() {
        Session.MapKey<Integer,String> key = Session.MapKey.mapNamed("foo");
        assertEquals("removal value for 1", null, session.remove(key, 1));
    }
}
