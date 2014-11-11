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

package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;

public interface NewAISProvider {
    /**
     * Gets the AIS that's been built.
     * @return the AIS
     */

    AkibanInformationSchema ais();
    /**
     * Gets the AIS that's been built.
     * @param freezeAIS whether to freeze the AIS before returning it
     * @return the AIS
     */
    AkibanInformationSchema ais(boolean freezeAIS);

    /**
     * Gets the AIS that's been built, but without performing
     * AIS validations. Used for building test schemas which may 
     * be invalid, but that's ok for testing purposes.  
     * @return the AIS
     */
    AkibanInformationSchema unvalidatedAIS();

    /**
     * <p>Defines (but does not yet start building) a group index.</p>
     *
     * <p>Note that this puts you into the realm of a cousin interface branch;
     * you can't alter the main schema anymore. This is by design, as implementations may need to differentiate
     * between structural building and building that depends on a stable structure (such as group index creation).</p>
     * @param indexName the new index's name
     * @param joinType the new index's join type
     * @return the group index builder
     */
    NewAISGroupIndexStarter groupIndex(String indexName, Index.JoinType joinType);
}
