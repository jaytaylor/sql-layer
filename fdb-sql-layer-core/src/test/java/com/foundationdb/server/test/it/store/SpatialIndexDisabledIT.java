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

package com.foundationdb.server.test.it.store;

import com.foundationdb.ais.model.Index.JoinType;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.NotAllowedByConfigException;
import com.foundationdb.server.service.config.TestConfigService;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SpatialIndexDisabledIT extends ITBase
{
    @Override
    protected Map<String, String> startupConfigProperties() {
        Map<String, String> map = new HashMap<>(super.startupConfigProperties());
        map.put(TestConfigService.FEATURE_SPATIAL_INDEX_KEY, "false");
        return map;
    }

    @Test(expected=NotAllowedByConfigException.class)
    public void tableIndex() {
        createTable("test", "t", "id int not null primary key, lat decimal(11,7), lon decimal(11,7)");
        createSpatialTableIndex("test", "t", "s", 0, 2, "lat", "lon");
    }

    @Test(expected=NotAllowedByConfigException.class)
    public void groupIndex() {
        createTable("test", "c", "id int not null primary key, x int");
        createTable("test", "a", "id int not null primary key, cid int, lat decimal(11,7), lon decimal(11,7), grouping foreign key(cid) references c(id)");
        createSpatialGroupIndex(new TableName("test", "c"), "s", 1, 2, JoinType.LEFT, "c.x", "a.lat", "a.lon");
    }
}
