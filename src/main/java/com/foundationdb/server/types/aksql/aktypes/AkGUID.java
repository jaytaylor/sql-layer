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

import java.util.UUID;

import static com.foundationdb.sql.types.TypeId.getUserDefinedTypeId;


public class AkGUID extends NoAttrTClass
    {
        public static final TypeId GUIDTYPE;
        static {
            try {
                // getUserDefinedTypeId will never throw StandardException
                GUIDTYPE = getUserDefinedTypeId("guid", false);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
        
        public final static NoAttrTClass INSTANCE = new AkGUID();
        public static final ValueCacher cacher = new GuidCacher();
        
        private AkGUID(){
           super(AkBundle.INSTANCE.id(), "guid", AkCategory.STRING_BINARY, TFormatter.FORMAT.GUID, 1,
                   1, 16, UnderlyingType.BYTES,
                   AkParsers.GUID, 36, GUIDTYPE);
        }
        
        @Override
        public ValueCacher cacher() {
            return cacher;
        }

        private static class GuidCacher implements ValueCacher {
            
            @Override
            public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
                byte[] bb = new byte[16];
                if (bdw instanceof UUID) {
                    UUID guid = (UUID)bdw;
                    AkServerUtil.putLong(bb, 0, guid.getMostSignificantBits());
                    AkServerUtil.putLong(bb, 8 , guid.getLeastSignificantBits());
                    target.putBytes(bb);                    
                } else {
                    throw new InvalidParameterValueException("cannot perform UUID cast on Object");
                }
            }

            @Override
            public Object valueToCache(BasicValueSource value, TInstance type) {
                byte[] bb = new byte[16];
                bb = value.getBytes();
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
                //copy(in, typeInstance, out);              
                UUID guid = (UUID)in.getObject();
                out.putBytes(uuidToBytes(guid));
            }

            @Override
            public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
                byte[] bb = in.getBytes();
                out.putObject(bytesToUUID(bb));
            }

            @Override
            public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
                copy(in, typeInstance, out);
            }
        };
        
        
        private static byte[] uuidToBytes(UUID guid) {
            byte[] bb = new byte[16];
            AkServerUtil.putLong(bb, 0, guid.getMostSignificantBits());
            AkServerUtil.putLong(bb, 8 , guid.getLeastSignificantBits());
            return bb;
        }

        private static UUID bytesToUUID(byte[] byteAr) {
            return new UUID(AkServerUtil.getLong(byteAr, 0), AkServerUtil.getLong(byteAr, 8));
        }
    }
        
