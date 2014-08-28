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

package com.foundationdb.server.types.aksql.aktypes;

import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

public class AkGUIDTest {

    @Test
    public void UUIDtoUUID() {
        UUID randomUUID = UUID.randomUUID();
        byte[] byteArray = AkGUID.uuidToBytes(randomUUID);
        UUID outputUUID = AkGUID.bytesToUUID(byteArray, 0);
        assertEquals(randomUUID,outputUUID);
    }

    @Test
    public void BytesToBytes() {
        byte byteArray[] = new byte[16];
        for(int i = 0; i < 16; i++){
            byteArray[i] = (byte)i;
        }
        UUID tempUUID = AkGUID.bytesToUUID(byteArray, 0);
        byte[] outputByteArray = AkGUID.uuidToBytes(tempUUID);
        assertArrayEquals(outputByteArray, byteArray);

    }

    @Test
    public void checkUUIDToBytes() {
        String uuidString = "384000008cf011bdb23e10b96e4ef00d";
        UUID uuid = UUID.fromString( "38400000-8cf0-11bd-b23e-10b96e4ef00d");
        uuidString.replace("-","");
        byte[] bytes = AkGUID.uuidToBytes(uuid);
        String output = Hex.encodeHexString(bytes);
        assertEquals(output, uuidString);
    }
}
