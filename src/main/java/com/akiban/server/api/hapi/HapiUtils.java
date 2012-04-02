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

package com.akiban.server.api.hapi;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiPredicate;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Iterator;

public class HapiUtils {
    public static String escape(String string) {
        if (string == null) {
            return "NULL";
        }
        if ("NULL".equalsIgnoreCase(string)) {
            return '\'' + string + '\'';
        }
        for (int i=0,len=string.length(); i < len; ++i) {
            char c = string.charAt(i);
            if (!(  (c >= 'a' && c <= 'z')
                  ||(c >= 'A' && c <= 'Z')
                  ||(c >= '0' && c <= '9')
                  ||(c == '_'))
                    )
            {
                try {
                    return '\'' + URLEncoder.encode(string, "UTF-8") + '\'';
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e); // shouldn't ever happen!
                }
            }
        }
        return string;
    }

    public static String toString(HapiGetRequest request) {
        StringBuilder builder = new StringBuilder();
        builder.append(HapiUtils.escape(request.getSchema())).append(':');
        builder.append(HapiUtils.escape(request.getTable())).append(':');
        boolean showPredicateTable = !request.getUsingTable().getTableName().equals(request.getTable());
        if (showPredicateTable || request.getLimit() >= 0) {
            builder.append('(');
            if (showPredicateTable) {
                builder.append(HapiUtils.escape(request.getUsingTable().getTableName()));
            } if (request.getLimit() >= 0) {
                builder.append(":LIMIT=").append(request.getLimit());
            }
            builder.append(')');
        }
        Iterator<HapiPredicate> predicatesIter = request.getPredicates().iterator();
        while (predicatesIter.hasNext()) {
            builder.append(predicatesIter.next());
            if (predicatesIter.hasNext()) {
                builder.append(',');
            }
        }
        return builder.toString();
    }

    public static boolean equals(HapiGetRequest self, HapiGetRequest that) {
        return self.getPredicates().equals(that.getPredicates())
                && self.getUsingTable().equals(that.getUsingTable())
                && self.getTable().equals(that.getTable())
                && self.getLimit() == that.getLimit()
                ;
    }

    public static int hashCode(HapiGetRequest self) {
        int result = self.getUsingTable().hashCode();
        result = 31 * result + self.getTable().hashCode();
        result = 31 * result + self.getPredicates().hashCode();
        result = 31 * result + self.getLimit();
        return result;
    }

    public static String toString(HapiPredicate predicate) {
        StringBuilder builder = new StringBuilder();
        builder.append(HapiUtils.escape(predicate.getColumnName()));
        builder.append(predicate.getOp());
        builder.append(HapiUtils.escape(predicate.getValue()));
        return builder.toString();
    }

    public static boolean equals(HapiPredicate one, HapiPredicate two) {
        if (! one.getColumnName().equals(two.getColumnName()) && one.getOp().equals(two.getOp()) )
            return false;
        if (one.getValue() == null)
            return two.getValue() == null;
        return one.getValue().equals(two.getValue());
    }

    public static int hashCode(HapiPredicate predicate) {
        int result = predicate.getTableName().hashCode();
        result = 31 * result + predicate.getColumnName().hashCode();
        result = 31 * result + predicate.getOp().hashCode();
        result = 31 * result + (predicate.getValue() != null ? predicate.getValue().hashCode() : 0);
        return result;
    }
}
