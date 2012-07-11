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

package com.akiban.sql.pg;

import com.akiban.sql.pg.PostgresEmulatedMetaDataStatement.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/** Handle known system table queries from tools directly.  At some
 * point it may be possible to actually implement <code>pg_</code>
 * tables as views against Akiban's own information schema. But for
 * now, some of the queries do not even parse in Akiban SQL.
 */
public class PostgresEmulatedMetaDataStatementParser implements PostgresStatementParser
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresEmulatedMetaDataStatementParser.class);

    /** Quickly determine whether a given query <em>might</em> be a
     * Postgres system table. */
    public static final String POSSIBLE_PG_QUERY = "FROM\\s+PG_";
    
    private Pattern possiblePattern;

    public PostgresEmulatedMetaDataStatementParser(PostgresServerSession server) {
        possiblePattern = Pattern.compile(POSSIBLE_PG_QUERY, Pattern.CASE_INSENSITIVE);
    }

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes)  {
        if (!possiblePattern.matcher(sql).find())
            return null;
        List<String> groups = new ArrayList<String>();
        for (Query query : Query.values()) {
            if (query.matches(sql, groups)) {
                logger.debug("Emulated: {}", query);
                return new PostgresEmulatedMetaDataStatement(query, groups);
            }
        }
        return null;
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
