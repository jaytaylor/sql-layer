
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
