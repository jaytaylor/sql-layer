/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AkibanInformationSchema;

public interface NewAISProvider {
    /**
     * Gets the AIS that's been built.
     * @return the AIS
     */
    AkibanInformationSchema ais();

    /**
     * <p>Defines (but does not yet start building) a group index.</p>
     *
     * <p>Note that this puts you into the realm of a cousin interface branch;
     * you can't alter the main schema anymore. This is by design, as implementations may need to differentiate
     * between structural building and building that depends on a stable structure (such as group index creation).</p>
     * @param indexName the new index's name
     * @return the group index builder
     */
    NewAISGroupIndexStarter groupIndex(String indexName);
}
