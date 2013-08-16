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

package com.foundationdb.sql.optimizer.rule;

import java.util.Properties;

public class PipelineConfiguration
{
    private boolean mapEnabled = false;
    private int indexScanLookaheadQuantum = 1;
    private int groupLookupLookaheadQuantum = 1;
    private boolean unionAllOpenBoth = false;
    private boolean selectBloomFilterEnabled = false;

    public PipelineConfiguration() {
    }

    public PipelineConfiguration(Properties properties) {
        load(properties);
    }

    public boolean isMapEnabled() {
        return mapEnabled;
    }

    public int getIndexScanLookaheadQuantum() {
        return indexScanLookaheadQuantum;
    }

    public int getGroupLookupLookaheadQuantum() {
        return groupLookupLookaheadQuantum;
    }

    public boolean isUnionAllOpenBoth() {
        return unionAllOpenBoth;
    }

    public boolean isSelectBloomFilterEnabled() {
        return selectBloomFilterEnabled;
    }

    public void load(Properties properties) {
        for (String prop : properties.stringPropertyNames()) {
            String val = properties.getProperty(prop);
            if ("map.enabled".equals(prop))
                mapEnabled = Boolean.parseBoolean(val);
            else if ("indexScan.lookaheadQuantum".equals(prop))
                indexScanLookaheadQuantum = Integer.parseInt(val);
            else if ("groupLookup.lookaheadQuantum".equals(prop))
                groupLookupLookaheadQuantum = Integer.parseInt(val);
            else if ("unionAll.openBoth".equals(prop))
                unionAllOpenBoth = Boolean.parseBoolean(val);
            else if ("selectBloomFilter.enabled".equals(prop))
                selectBloomFilterEnabled = Boolean.parseBoolean(val);
            else
                throw new IllegalArgumentException("Unknown property " + prop);
        }
    }
}
