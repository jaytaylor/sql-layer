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

/** Geometry distance (e.g., circle containment) predicate. */
public class GeoWithinDistance extends TScalarBase
{
    private final TClass geometryType;
    private final TClass radiusType;

    public GeoWithinDistance(TClass geometryType, TClass radiusType) {
        this.geometryType = geometryType;
        this.radiusType = radiusType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(geometryType, 0).covers(geometryType, 1).covers(radiusType, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        Geometry geo1 = (Geometry)inputs.get(0).getObject();
        Geometry geo2 = (Geometry)inputs.get(1).getObject();
        double radius = inputs.get(2).getDouble();
        output.putBool(geo1.isWithinDistance(geo2, radius));
    }

    @Override
    public String displayName() {
        return "GEO_WITHIN_DISTANCE";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(AkBool.INSTANCE);
    }
}
