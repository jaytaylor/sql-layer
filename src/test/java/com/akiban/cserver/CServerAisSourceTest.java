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

package com.akiban.cserver;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.io.MessageTarget;
import com.akiban.ais.io.Reader;
import com.akiban.ais.io.Writer;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Source;
import com.akiban.ais.model.Target;

public class CServerAisSourceTest extends CServerTestCase implements CServerConstants {

    private final static String DDL_FILE_NAME = "data_dictionary_test.ddl";

    private AkibanInformationSchema ais;

    @Before
    public void setUp() throws Exception {
        baseSetUp();
        ais = setUpAisForTests(DDL_FILE_NAME);
    }

    @After
    public void tearDown() throws Exception {
        baseTearDown();
    }

    @Test
    public void testCServerAis() throws Exception {
        // Store AIS data in Chunk Server
        final Target target = new CServerAisTarget(store);
        new Writer(target).save(ais);

        // Retrieve AIS data from Chunk Server
        final Source source = new CServerAisSource(store);
        final AkibanInformationSchema aisCopy = new Reader(source).load();

        // new Writer(new SqlTextTarget(new PrintWriter(new
        // FileWriter("/tmp/ais1.txt")))).save(ais);
        // new Writer(new SqlTextTarget(new PrintWriter(new
        // FileWriter("/tmp/ais2.txt")))).save(aisCopy);

        assertTrue(equals(ais, aisCopy));
    }

    @Test
    public void testReloadAIS() throws Exception {
        // Store AIS data in Chunk Server
        final Target target = new CServerAisTarget(store);
        new Writer(target).save(ais);

        // Retrieve AIS data from Chunk Server
        final Source source1 = new CServerAisSource(store);
        final AkibanInformationSchema aisCopy1 = new Reader(source1).load();
        new Writer(target).save(aisCopy1);

        final Source source2 = new CServerAisSource(store);
        final AkibanInformationSchema aisCopy2 = new Reader(source2).load();

        // new Writer(new SqlTextTarget(new PrintWriter(new
        // FileWriter("/tmp/ais1.txt")))).save(ais);
        // new Writer(new SqlTextTarget(new PrintWriter(new
        // FileWriter("/tmp/ais2.txt")))).save(aisCopy2);

        assertTrue(equals(ais, aisCopy2));

        final Source source3 = new CServerAisSource(store);
        final AkibanInformationSchema aisCopy3 = new Reader(source3).load();
        assertTrue(equals(ais, aisCopy3));
    }

    private boolean equals(final AkibanInformationSchema ais1,
            final AkibanInformationSchema ais2) throws Exception {
        final ByteBuffer bb1 = ByteBuffer.allocate(100000);
        final ByteBuffer bb2 = ByteBuffer.allocate(100000);

        new Writer(new MessageTarget(bb1)).save((AkibanInformationSchema) ais1);
        new Writer(new MessageTarget(bb2)).save((AkibanInformationSchema) ais2);
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
