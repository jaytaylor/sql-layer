
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
            // Don't return the exchange to the adapter. TreeServiceImpl caches it for the tree, and we're done
            // with the tree. Calling adapter.returnExchange would cause a leak of exchanges.
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
