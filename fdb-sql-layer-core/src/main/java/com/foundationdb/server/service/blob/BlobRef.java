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

import com.foundationdb.server.error.LobException;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;

import java.util.UUID;
import java.util.Arrays;

public class BlobRef {
    public enum LeadingBitState { NO, YES, UNKNOWN }
    public enum LobType { SHORT_LOB, LONG_LOB, UNKNOWN }
    public static final byte SHORT_LOB = 0x01;
    public static final byte LONG_LOB = 0x02;
    private UUID id;
    private byte[] data;
    private byte storeTypeBit;
    private LeadingBitState leadingBitState = LeadingBitState.UNKNOWN;
    private LobType lobType = LobType.UNKNOWN;
    private LobType requestedType = LobType.UNKNOWN;
    private Boolean returnedBlobInSimpleMode = false;
    
    
    public BlobRef(byte[] value) {
        this(value, LeadingBitState.UNKNOWN);
    }
    public BlobRef(byte[] value, LeadingBitState state) {
        this(value, state, LobType.UNKNOWN, LobType.UNKNOWN);
    }
    

    public BlobRef(byte[] value, LeadingBitState state, LobType definedType, LobType requestedType) {
        this.leadingBitState = state;
        this.lobType = definedType;
        this.requestedType = requestedType;
        
        if (leadingBitState == LeadingBitState.YES) {
            storeTypeBit = value[0];
            if (storeTypeBit == SHORT_LOB) {
                lobType = LobType.SHORT_LOB;
            } else if (storeTypeBit == LONG_LOB) {
                lobType = LobType.LONG_LOB;
            } else {
                throw new LobException("Invalid leading bit -");                
            }
            
            if (isShortLob()) {
                data = Arrays.copyOfRange(value, 1, value.length);
            } else if (isLongLob()) {
                if (value.length != 17){
                    throw new LobException("invalid id length");
                }
                id = AkGUID.bytesToUUID(value, 1);
            } else {
                throw new LobException("Invalid store type");
            }
        } else {
            data = value;
        }
    }
    
    public byte[] getValue() {
        // always returns data with the correct leading bit if applicable
        byte[] res;
        if (leadingBitState == LeadingBitState.YES) {
            if (isShortLob()) {
                res = new byte[data.length + 1];
                System.arraycopy(data, 0, res, 1, data.length);
                res[0] = storeTypeBit;
            }
            else {
                res = new byte[17];
                System.arraycopy(AkGUID.uuidToBytes(id), 0, res, 1, 16);
                res[0] = storeTypeBit;
            }
        } else {
            res = data;
        }
        return res;
    }
    
    public boolean isShortLob() {
        return lobType == LobType.SHORT_LOB;
    }

    public boolean isLongLob() {
        return lobType == LobType.LONG_LOB;
    }
    
    public UUID getId() {
        return id;
    }
    
    public void setId(UUID id) {
        this.id = id;
    }
    
    public byte[] getBytes() {
        return data;
    }
    
    public LobType getLobType() {
        return lobType;
    }
    
    public void setLobType(LobType lobType) {
        this.lobType = lobType;
    }
    
    public LobType getRequestedLobType() { return requestedType; }
    
    public Boolean isReturnedBlobInSimpleMode() {
        return returnedBlobInSimpleMode;
    }
    
    public void setIsReturnedBlobInSimpleMode(Boolean value) {
        returnedBlobInSimpleMode = value;
    }
}
