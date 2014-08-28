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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.common.TFormatter;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.*;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.server.AkServerUtil;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.foundationdb.sql.types.TypeId.getUserDefinedTypeId;


public class AkGUID extends NoAttrTClass
    {
        public final static TypeId GUIDTYPE = TypeId.GUID_ID;
        public final static NoAttrTClass INSTANCE = new AkGUID();
        public final static ValueCacher CACHER = new GuidCacher();
        
        private AkGUID(){
           super(AkBundle.INSTANCE.id(), GUIDTYPE.getSQLTypeName(), AkCategory.STRING_BINARY, TFormatter.FORMAT.GUID, 1,
                   1, 16, UnderlyingType.BYTES,
                   AkParsers.GUID, GUIDTYPE.getMaximumMaximumWidth(), GUIDTYPE);
        }
        
        @Override
        public ValueCacher cacher() {
            return CACHER;
        }

        private static class GuidCacher implements ValueCacher {
            
            @Override
            public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
                if (bdw instanceof UUID) {
                    byte[] bb = uuidToBytes((UUID)bdw);
                    target.putBytes(bb);                    
                } else {
                    throw new InvalidParameterValueException("cannot perform UUID cast on Object");
                }
            }

            @Override
            public Object valueToCache(BasicValueSource value, TInstance type) {
                byte[] bb = value.getBytes();
                return new UUID(AkServerUtil.getLong(bb, 0), AkServerUtil.getLong(bb, 8));
            }

            @Override
            public Object sanitize(Object object) {
                return object;
            }

            @Override
            public boolean canConvertToValue(Object cached) {
                return true;
            }
        }

        @Override
        protected ValueIO getValueIO() {
            return valueIO;
        }
        
        private static final ValueIO valueIO = new ValueIO() {
            
            protected void copy(ValueSource in, TInstance typeInstance, ValueTarget out) {
                ValueTargets.copyFrom(in, out);
            }
            
            @Override
            public void writeCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
                UUID guid = (UUID)in.getObject();
                out.putBytes(uuidToBytes(guid));
            }

            @Override
            public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
                byte[] bb = in.getBytes();
                out.putObject(bytesToUUID(bb, 0));
            }

            @Override
            public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
                copy(in, typeInstance, out);
            }
        };
        
        
        public static byte[] uuidToBytes(UUID guid) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, guid.getMostSignificantBits());
            bb.putLong(8, guid.getLeastSignificantBits());
            return bb.array();
        }

        public static UUID bytesToUUID(byte[] byteAr, int offset) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.put(byteAr);
            return new UUID(bb.getLong(offset), bb.getLong(offset + 8));
        }
    }
        
