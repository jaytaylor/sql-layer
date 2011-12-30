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

package com.akiban.server.test.it;

import static org.junit.Assert.assertTrue;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import com.akiban.ais.ddl.SqlTextTarget;
import com.akiban.server.AkServerAisSource;
import com.akiban.server.AkServerAisTarget;
import com.akiban.server.test.it.store.DataDictionaryDDL;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.ais.model.Target;

public class AkServerAisSourceTargetIT extends ITBase {
    @Before
    public void setUp() throws Exception {
        DataDictionaryDDL.createTables(session(), ddl());
    }

    @Test
    public void testAkServerAis() throws Exception {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                final AkibanInformationSchema ais = ddl().getAIS(session());

                // Store AIS data
                final Target target = new AkServerAisTarget(store(), serviceManager().getSessionService());
                new Writer(target).save(ais);

                // Retrieve AIS data
                final Source source = new AkServerAisSource(store(), serviceManager().getSessionService());
                final AkibanInformationSchema aisCopy = new Reader(source).load();

                new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais1.txt")))).save(ais);
                new Writer(new SqlTextTarget(new PrintWriter(new FileWriter("/tmp/ais2.txt")))).save(aisCopy);
                assertTrue(areEqual(ais, aisCopy));

                return null;
            }
        });
    }

    @Test
    public void testReloadAIS() throws Exception {
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                final AkibanInformationSchema ais = ddl().getAIS(session());

                // Store AIS data
                final Target target = new AkServerAisTarget(store(), serviceManager().getSessionService());
                new Writer(target).save(ais);

                // Retrieve AIS data
                final Source source1 = new AkServerAisSource(store(), serviceManager().getSessionService());
                final AkibanInformationSchema aisCopy1 = new Reader(source1).load();
                new Writer(target).save(aisCopy1);

                final Source source2 = new AkServerAisSource(store(), serviceManager().getSessionService());
                final AkibanInformationSchema aisCopy2 = new Reader(source2).load();

                assertTrue(areEqual(ais, aisCopy2));

                final Source source3 = new AkServerAisSource(store(), serviceManager().getSessionService());
                final AkibanInformationSchema aisCopy3 = new Reader(source3).load();
                assertTrue(areEqual(ais, aisCopy3));

                return null;
            }
        });
    }

    private boolean areEqual(final AkibanInformationSchema ais1, final AkibanInformationSchema ais2) throws Exception {
        final ByteBuffer bb1 = ByteBuffer.allocate(100000);
        final ByteBuffer bb2 = ByteBuffer.allocate(100000);

        new Writer(new MessageTarget(bb1)).save(ais1);
        new Writer(new MessageTarget(bb2)).save(ais2);
        bb1.flip();
        bb2.flip();
        if (bb1.limit() != bb2.limit()) {
            return false;
        }
        for (int i = 0; i < bb1.limit(); i++) {
            if (bb1.get() != bb2.get()) {
                return false;
            }
        }
        return true;
    }

}
