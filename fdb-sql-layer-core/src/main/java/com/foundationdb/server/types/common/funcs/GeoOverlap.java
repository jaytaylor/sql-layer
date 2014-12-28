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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Geometry;

/** Simple predicates between two {@link Geometry} objects. */
public class GeoOverlap extends TScalarBase
{
    public enum OverlapType {
        GEO_OVERLAP, GEO_CONTAIN, GEO_COVER;

        public String functionName() {
            return name();
        }
    }

    public static TScalar[] create(TClass geometryType) {
        TScalar[] funs = new TScalar[OverlapType.values().length];
        for (int i = 0; i < funs.length; i++) {
            funs[i] = new GeoOverlap(geometryType, OverlapType.values()[i]);
        }
        return funs;
    }

    private final TClass geometryType;
    private final OverlapType overlapType;

    public GeoOverlap(TClass geometryType, OverlapType overlapType) {
        this.geometryType = geometryType;
        this.overlapType = overlapType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(geometryType, 0).covers(geometryType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        Geometry geo1 = (Geometry)inputs.get(0).getObject();
        Geometry geo2 = (Geometry)inputs.get(1).getObject();
        boolean result = false;
        switch (overlapType) {
        case GEO_OVERLAP:
            result = geo1.overlaps(geo2);
            break;
        case GEO_CONTAIN:
            result = geo1.contains(geo2);
            break;
        case GEO_COVER:
            result = geo1.covers(geo2);
            break;
        }
        output.putBool(result);
    }

    @Override
    public String displayName() {
        return overlapType.functionName();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
}
