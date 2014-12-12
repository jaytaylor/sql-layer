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

import com.foundationdb.server.error.*;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.aksql.*;
import com.foundationdb.server.types.common.*;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.*;
import com.foundationdb.sql.types.*;

import java.util.*;


public class AkBlob extends NoAttrTClass {

    public final static TypeId BLOBTYPE = TypeId.BLOB_ID;
    public final static NoAttrTClass INSTANCE = new AkBlob();
    public final static ValueCacher CACHER = new BlobCacher();
    
    private AkBlob(){
        super(AkBundle.INSTANCE.id(), BLOBTYPE.getSQLTypeName(), AkCategory.STRING_BINARY, TFormatter.FORMAT.BLOB, 1,
                1, 16, UnderlyingType.BYTES,
                AkParsers.BLOB, BLOBTYPE.getMaximumMaximumWidth(), BLOBTYPE);
    }

    
    @Override
    public ValueCacher cacher() {
        return CACHER;
    }

    private static class BlobCacher implements ValueCacher {

        @Override
        public void cacheToValue(Object bdw, TInstance type, BasicValueTarget target) {
            if (bdw instanceof UUID) {
                byte[] bb = AkGUID.uuidToBytes((UUID) bdw);
                target.putBytes(bb);
            } else {
                throw new InvalidParameterValueException("cannot perform Blob-id cast on Object");
            }
        }

        @Override
        public Object valueToCache(BasicValueSource value, TInstance type) {
            byte[] bb = value.getBytes();
            return AkGUID.bytesToUUID(bb, 0);
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
            out.putBytes(AkGUID.uuidToBytes(guid));
        }

        @Override
        public void readCollating(ValueSource in, TInstance typeInstance, ValueTarget out) {
            byte[] bb = in.getBytes();
            out.putObject(AkGUID.bytesToUUID(bb, 0));
        }

        @Override
        public void copyCanonical(ValueSource in, TInstance typeInstance, ValueTarget out) {
            copy(in, typeInstance, out);
        }
    };
}
