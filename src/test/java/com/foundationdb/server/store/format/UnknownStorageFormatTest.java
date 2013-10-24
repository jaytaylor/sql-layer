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

package com.foundationdb.server.store.format;

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.server.store.format.DummyStorageFormatRegistry;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.util.GrowableByteBuffer;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class UnknownStorageFormatTest
{
    private StorageFormatRegistry testFormatRegistry = DummyStorageFormatRegistry.create();
    private GrowableByteBuffer bytes = new GrowableByteBuffer(4096);

    @Before
    public void saveWithExtension() {
        TestStorageFormatExtended.register(testFormatRegistry);

        AISBuilder aisb = new AISBuilder();
        Sequence sequence = aisb.sequence("test", "seq", 0, 1, 0, 1000, true);
        TestStorageDescriptionExtended storageDescription = new TestStorageDescriptionExtended(sequence);
        storageDescription.setStorageKey("KEY");
        storageDescription.setExtension("PLUS");
        assertTrue(isFullDescription(storageDescription));
        sequence.setStorageDescription(storageDescription);
        new ProtobufWriter(bytes).save(aisb.akibanInformationSchema());
        bytes.flip();
    }

    @Test
    public void loadNormally() {
        Sequence sequence = loadSequence(testFormatRegistry);
        assertNotNull(sequence);
        assertTrue(isFullDescription(sequence.getStorageDescription()));
    }

    @Test
    public void loadPartially() {
        StorageFormatRegistry newFormatRegistry = DummyStorageFormatRegistry.create();
        Sequence sequence = loadSequence(newFormatRegistry);
        assertNotNull(sequence);
        assertFalse(isFullDescription(sequence.getStorageDescription()));
        assertTrue(isPartialDescription(sequence.getStorageDescription()));
    }

    @Test
    public void reloadNormally() {
        StorageFormatRegistry newFormatRegistry = DummyStorageFormatRegistry.create();
        AkibanInformationSchema ais = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(newFormatRegistry, ais);
        reader.loadBuffer(bytes);
        reader.loadAIS();
        bytes.flip();
        new ProtobufWriter(bytes).save(ais);
        bytes.flip();
        Sequence sequence = loadSequence(testFormatRegistry);
        assertNotNull(sequence);
        assertTrue(isFullDescription(sequence.getStorageDescription()));
    }

    protected Sequence loadSequence(StorageFormatRegistry storageFormatRegistry) {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        ProtobufReader reader = new ProtobufReader(storageFormatRegistry, ais);
        reader.loadBuffer(bytes);
        reader.loadAIS();
        return ais.getSequence(new TableName("test", "seq"));
 }

    protected boolean isFullDescription(StorageDescription storageDescription) {
        return (isPartialDescription(storageDescription) &&
                (storageDescription instanceof TestStorageDescriptionExtended) &&
                "PLUS".equals(((TestStorageDescriptionExtended)storageDescription).getExtension()));
    }

    protected boolean isPartialDescription(StorageDescription storageDescription) {
        return ((storageDescription instanceof TestStorageDescription) &&
                "KEY".equals(((TestStorageDescription)storageDescription).getStorageKey()));
    }

}
