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
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

/** Parse a Well-Known binary (WKB) byte string into a geometry object. Thin wrapper around {@link WKBReader}. */
public class GeoWKB extends TScalarBase
{
    private static final int READER_CONTEXT_POS = 0;

    private final TClass binaryType;
    private final TClass geometryType;

    public GeoWKB(TClass binaryType, TClass geometryType) {
        this.binaryType = binaryType;
        this.geometryType = geometryType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(binaryType, 0);
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        context.set(READER_CONTEXT_POS, new WKBReader());
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        byte[] wkb = inputs.get(0).getBytes();
        WKBReader reader = (WKBReader)context.preptimeObjectAt(READER_CONTEXT_POS);
        try {
            Geometry geometry = reader.read(wkb);
            output.putObject(geometry);
        } catch(ParseException e) {
            throw new InvalidSpatialObjectException(e.getMessage());
        }
    }

    @Override
    public String displayName() {
        return "GEO_WKB";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(geometryType);
    }
}
