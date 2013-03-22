
package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;

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
     * <p>Defines (but does not yet start building) a LEFT JOIN group index.</p>
     *
     * <p>Note that this puts you into the realm of a cousin interface branch;
     * you can't alter the main schema anymore. This is by design, as implementations may need to differentiate
     * between structural building and building that depends on a stable structure (such as group index creation).</p>
     * @param indexName the new index's name
     * @return the group index builder
     * @deprecated use {@link #groupIndex(String, Index.JoinType)} instead
     */
    @Deprecated
    NewAISGroupIndexStarter groupIndex(String indexName);

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
