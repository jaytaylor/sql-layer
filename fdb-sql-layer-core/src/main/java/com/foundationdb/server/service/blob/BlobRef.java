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

package com.foundationdb.server.service.blob;

import com.foundationdb.server.error.*;
import com.foundationdb.server.types.aksql.aktypes.*;

import java.util.UUID;
import java.util.Arrays;

public class BlobRef {
    public static final byte SHORT_LOB = 0x01;
    public static final byte LONG_LOB = 0x02;
    private UUID id;
    private byte[] data;
    private byte storeType;
    
    public BlobRef(byte[] value){
        storeType = value[0];
        if (storeType == SHORT_LOB) {
            data = Arrays.copyOfRange(value, 1, value.length);
        } else if (storeType == LONG_LOB) {
            id = AkGUID.bytesToUUID(value, 1);
        } else {
            throw new LobException("Invalid store type");
        }
    }
    
    public BlobRef(UUID id){
        this.id = id;
        this.storeType = LONG_LOB;
    }
    
    public BlobRef(UUID id, byte[] data, byte storageType) {
        this.id = id;
        this.data = data;
        this.storeType = storageType;
    }
    
    public byte[] getIdOrBytes() {
        if (storeType == SHORT_LOB) {
            return data;
        } else {
            return AkGUID.uuidToBytes(id);
        }
    }
    
    public byte[] getValue() {
        byte[] res;
        if (storeType == SHORT_LOB) {
            res = new byte[data.length + 1];
            System.arraycopy(data, 0, res, 1, data.length);
        }
        else {
            res = new byte[17];
            System.arraycopy(AkGUID.uuidToBytes(id), 0, res, 1, 16);
        }
        res[0] = storeType;
        return res;
    }
    
    public boolean isShortLob() {
        return storeType == SHORT_LOB;
    }

    public boolean isLongLob() {
        return storeType == LONG_LOB;
    }
    
    public UUID getId() {
        return id;
    }
    
    public byte[] getBytes() {
        return data;
    }
}
