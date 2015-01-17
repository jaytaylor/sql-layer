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

import com.foundationdb.server.error.InvalidSpatialObjectException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import static com.foundationdb.server.types.common.funcs.DistanceLatLon.*;

/** Construct a Point {@link Geometry} object from two coordinates. */
public class GeoLatLon extends TScalarBase
{
    private static final int FACTORY_CONTEXT_POS = 0;

    private final TClass coordType;
    private final TClass geometryType;
    
    public GeoLatLon(TClass coordType, TClass geometryType) {
        this.coordType = coordType;
        this.geometryType = geometryType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(coordType, 0).covers(coordType, 1);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        context.set(FACTORY_CONTEXT_POS, new GeometryFactory());
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        ValueSource input0 = inputs.get(0);
        ValueSource input1 = inputs.get(1);

        GeometryFactory factory = (GeometryFactory)context.preptimeObjectAt(FACTORY_CONTEXT_POS);
        double lat = doubleInRange(TBigDecimal.getWrapper(input0, input0.getType()), MIN_LAT, MAX_LAT);
        double lon = doubleInRange(TBigDecimal.getWrapper(input1, input1.getType()), MIN_LON, MAX_LON);
        Geometry geometry = factory.createPoint(new Coordinate(lat, lon));
        output.putObject(geometry);
    }

    @Override
    public String displayName() {
        return "GEO_LAT_LON";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(geometryType);
    }
}
