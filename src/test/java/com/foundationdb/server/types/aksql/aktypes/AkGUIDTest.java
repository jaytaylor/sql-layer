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

import java.util.Arrays;
import java.util.UUID;
import org.junit.Test;

public class AkGUIDTest {

    @Test
    public void UUIDtoUUID() {
        UUID randomUUID = UUID.randomUUID();
        byte[] byteArray = AkGUID.uuidToBytes(randomUUID);
        UUID outputUUID = AkGUID.bytesToUUID(byteArray, 0);
        assert(randomUUID.equals(outputUUID));
    }

    @Test
    public void BytesToBytes() {
        byte byteArray[] = new byte[16];
        for(int i = 0; i < 16; i++){
            byteArray[i] = (byte)i;
        }
        UUID tempUUID = AkGUID.bytesToUUID(byteArray, 0);
        byte[] outputByteArray = AkGUID.uuidToBytes(tempUUID);
        assert(Arrays.equals(outputByteArray, byteArray));

    }
}
