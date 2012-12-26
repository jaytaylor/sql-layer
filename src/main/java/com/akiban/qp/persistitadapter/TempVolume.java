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

package com.akiban.qp.persistitadapter;

import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempVolume
{
    public static Exchange takeExchange(PersistitStore store, Session session, String treeName)
    {
        try {
            Persistit persistit = store.getDb();
            TempVolumeState tempVolumeState = session.get(TEMP_VOLUME_STATE);
            if (tempVolumeState == null) {
                // Persistit creates a temp volume per "Persistit session", and these are currently one-to-one with threads.
                // Conveniently, server sessions and threads are also one-to-one. If either of these relationships ever
                // change, then the use of session resources and temp volumes will need to be revisited. But for now,
                // persistit.createTemporaryVolume creates a temp volume that is private to the persistit session and
                // therefore to the server session.
                Volume volume = persistit.createTemporaryVolume();
                tempVolumeState = new TempVolumeState(volume);
                session.put(TEMP_VOLUME_STATE, tempVolumeState);
            }
            tempVolumeState.addUser();
            return new Exchange(persistit, tempVolumeState.volume(), treeName, true);
        } catch (PersistitException e) {
            if (!PersistitAdapter.isFromInterruption(e))
                LOG.error("Caught exception while getting exchange for sort", e);
            PersistitAdapter.handlePersistitException(session, e);
            assert false; // handlePersistitException should throw something
            return null;
        }
    }

    public static void returnExchange(Session session, Exchange exchange)
    {
        if (exchange != null) {
            try {
                TempVolumeState tempVolumeState = session.get(TEMP_VOLUME_STATE);
                int users = tempVolumeState.removeUser();
                if (users == 0) {
                    // Returns disk space used by the volume
                    tempVolumeState.volume().close();
                    session.remove(TEMP_VOLUME_STATE);
                }
            } catch (PersistitException e) {
                PersistitAdapter.handlePersistitException(session, e);
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TempVolume.class);
    private static final Session.Key<TempVolumeState> TEMP_VOLUME_STATE = Session.Key.named("TEMP_VOLUME_STATE");

    // public so that tests can see it
    public static class TempVolumeState
    {
        public TempVolumeState(Volume volume)
        {
            this.volume = volume;
            users = 0;
        }

        public Volume volume()
        {
            return volume;
        }

        public void addUser()
        {
            users++;
        }

        public int removeUser()
        {
            users--;
            assert users >= 0;
            return users;
        }

        private final Volume volume;
        private int users;
    }
}
