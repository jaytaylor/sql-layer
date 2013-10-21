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

package com.foundationdb.qp.storeadapter;

import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TempVolume
{
    public static Exchange takeExchange(PersistitStore store, Session session, String treeName)
    {
        boolean success = false;
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
            tempVolumeState.acquire();
            if(injectIOException) {
                throw new PersistitIOException(new IOException());
            }
            Exchange ex = new Exchange(persistit, tempVolumeState.volume(), treeName, true);
            success = true;
            return ex;
        } catch (PersistitException | RollbackException e) {
            if (!PersistitAdapter.isFromInterruption(e))
                LOG.debug("Caught exception while getting exchange for sort", e);
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            if (!success)
                releaseAndCloseIfUnshared(session);
        }
    }

    public static void returnExchange(Session session, Exchange exchange)
    {
        if (exchange != null) {
            releaseAndCloseIfUnshared(session);
            // Don't return the exchange to the adapter. TreeServiceImpl caches it for the tree, and we're done
            // with the tree. Calling adapter.returnExchange would cause a leak of exchanges.
        }
    }

    private static void releaseAndCloseIfUnshared(Session session) {
        TempVolumeState tempVolumeState = session.get(TEMP_VOLUME_STATE);
        tempVolumeState.release();
        if (!tempVolumeState.isShared())
        {
            session.remove(TEMP_VOLUME_STATE);
            try {
                // Returns disk space used by the volume
                tempVolumeState.volume().close();
            } catch(PersistitException | RollbackException e) {
                throw PersistitAdapter.wrapPersistitException(session, e);
            }
        }
    }

    // Public for tests

    public static boolean hasTempState(Session session)
    {
        return session.get(TEMP_VOLUME_STATE) != null;
    }

    public static void setInjectIOException(boolean injectIOException)
    {
        TempVolume.injectIOException = injectIOException;
    }

    private static final Logger LOG = LoggerFactory.getLogger(TempVolume.class);
    private static final Session.Key<TempVolumeState> TEMP_VOLUME_STATE = Session.Key.named("TEMP_VOLUME_STATE");
    private static volatile boolean injectIOException = false;

    private static class TempVolumeState
    {
        public TempVolumeState(Volume volume)
        {
            this.volume = volume;
            refCount = 0;
        }

        public Volume volume()
        {
            return volume;
        }

        public void acquire()
        {
            ++refCount;
        }

        public boolean isShared()
        {
            return refCount > 0;
        }

        public void release()
        {
            --refCount;
            assert refCount >= 0;
        }

        private final Volume volume;
        private int refCount;
    }
}
