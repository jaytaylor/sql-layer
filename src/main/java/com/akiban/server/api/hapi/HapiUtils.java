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
        if (!request.getUsingTable().getTableName().equals(request.getTable())) {
            builder.append('(').append(HapiUtils.escape(request.getUsingTable().getTableName())).append(')');
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
