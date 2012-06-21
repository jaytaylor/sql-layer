/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
