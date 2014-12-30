/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.aksql.aktypes.AkGeometry;
import com.foundationdb.server.types.common.funcs.GeoExpandedEnvelope;
import com.foundationdb.server.types.common.funcs.GeoLatLon;
import com.foundationdb.server.types.common.funcs.GeoOverlaps;
import com.foundationdb.server.types.common.funcs.GeoTypeString;
import com.foundationdb.server.types.common.funcs.GeoWKB;
import com.foundationdb.server.types.common.funcs.GeoWKT;
import com.foundationdb.server.types.common.funcs.GeoWithinDistance;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;

@SuppressWarnings("unused")
public class MGeometryFunctions
{
    // TODO: VARBINARY not allowed for index definition currently.
    public static final TScalar MGEOWKB = new GeoWKB(MBinary.BLOB, AkGeometry.INSTANCE);
    public static final TScalar MGEOWKT = new GeoWKT(MString.VARCHAR, AkGeometry.INSTANCE);
    public static final TScalar MGEOTYPESTRING = new GeoTypeString(MString.VARCHAR, AkGeometry.INSTANCE);

    public static final TScalar[] MGEOOVERLAPS = GeoOverlaps.create(AkGeometry.INSTANCE);

    public static final TScalar MGEOLATLON = new GeoLatLon(MNumeric.DECIMAL, AkGeometry.INSTANCE);

    public static final TScalar MGEOWITHINDISTANCE = new GeoWithinDistance(AkGeometry.INSTANCE, MApproximateNumber.DOUBLE);

    public static final TScalar MGEOEXPANDEDENVELOPE = new GeoExpandedEnvelope(AkGeometry.INSTANCE, MApproximateNumber.DOUBLE);

    private MGeometryFunctions() {}
}
