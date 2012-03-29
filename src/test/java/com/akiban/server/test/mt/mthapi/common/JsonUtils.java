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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.test.mt.mthapi.base.sais.SaisFK;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.util.ArgumentValidation;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.akiban.server.test.mt.mthapi.common.HapiValidationError.Reason.*;
import static com.akiban.server.test.mt.mthapi.common.HapiValidationError.*;
import static org.junit.Assert.*;

public final class JsonUtils {
    static Set<String> jsonObjectKeys(JSONObject jsonObject) {
        Iterator iter = jsonObject.keys();
        Set<String> keys = new HashSet<String>();
        while (iter.hasNext()) {
            String key = (String) iter.next();
            if (!keys.add(key)) {
                fail(String.format("dupliate key %s in %s", key, jsonObject));
            }
        }
        return keys;
    }

    static int jsonObjectInt(JSONObject jsonObject, String key, HapiGetRequest request) {
        assertFalse("<" + request + "> " + key + " null: " + jsonObject, jsonObject.isNull(key));
        try {
            return jsonObject.getInt(key);
        } catch (JSONException e) {
            throw new RuntimeException("<" + request + "> extracting " + key + " from " + jsonObject.toString(), e);
        }
    }

    public static void validateResponse(JSONObject response, SaisTable selectRoot, SaisTable predicateTable) {
        assertNotNull(RESPONSE_IS_NULL, response);
        ArgumentValidation.notNull("select root", selectRoot);
        ArgumentValidation.notNull("predicate table", predicateTable);

        Set<String> roots = jsonObjectKeys(response);
        assertEquals(ROOT_TABLES_COUNT, "number of root elements: " + roots, 1, roots.size());
        StringBuilder scratch = new StringBuilder();
        final String rootTableKey = escapeTable(selectRoot, scratch);
        assertEquals(ROOT_TABLE_NAME, "root table name", rootTableKey, roots.iterator().next());

        boolean rootIsPredicate = selectRoot.equals(predicateTable);
        try {
            JSONArray rootObjects = response.getJSONArray(rootTableKey);
            final int rootObjectsCount = rootObjects.length();
            if (rootObjectsCount == 0) {
                // no rows found, which is fine
                return;
            }
            if (!rootIsPredicate) {
                assertEquals(ROOT_ELEMENTS_COUNT, "number of root elements", 1, rootObjectsCount);
            }
            for (int rootObjectIndex=0; rootObjectIndex < rootObjectsCount; ++rootObjectIndex) {
                JSONObject rootObject = rootObjects.getJSONObject(rootObjectIndex);
                final boolean sawPredicates = validateResponseRecursively(rootObject, selectRoot, predicateTable,
                        rootIsPredicate, scratch);
                assertTrue(UNSEEN_PREDICATES, "never saw predicates for elem " + rootObjectsCount, sawPredicates);
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean validateResponseRecursively(JSONObject jsonObject, SaisTable atTable, SaisTable predicates,
                                                       boolean sawPredicateTable, StringBuilder scratch)
            throws JSONException
    {
        validateFields(atTable.getName(), atTable.getFields(), jsonObject, scratch);
        return validateChildren(jsonObject, atTable, predicates, sawPredicateTable, scratch);
    }

    private static boolean validateChildren(JSONObject jsonObject, SaisTable atTable, SaisTable predicates,
                                            boolean sawPredicateTable, StringBuilder scratch)
            throws JSONException
    {
        Map<String,SaisFK> childKeys = new HashMap<String, SaisFK>();
        for (SaisFK childFK : atTable.getChildren()) {
            SaisTable child = childFK.getChild();
            String tableKey = escapeTable(child, scratch);
            if (jsonObject.has(tableKey)) {
                childKeys.put(tableKey,childFK);
            }
        }
        if (!sawPredicateTable) {
            assertEquals(UNSEEN_PREDICATES, "child tables (when the predicate table hasn't been seen yet)", 1, childKeys.size());
        }
        if (childKeys.isEmpty()) {
            return sawPredicateTable;
        }
        List<Object> atTablePK = getPK(jsonObject, atTable, scratch);
        boolean ret = sawPredicateTable;
        for(Map.Entry<String,SaisFK> childKeyEntry : childKeys.entrySet()) {
            final String childKey = childKeyEntry.getKey();
            final JSONArray childArray;
            childArray = jsonObject.getJSONArray(childKey);
            if (!sawPredicateTable){
                assertEquals(UNSEEN_PREDICATES, "child array " + atTable + childKey, 1, childArray.length());
            }
            final SaisFK childFK = childKeyEntry.getValue();
            final SaisTable childSais = childFK.getChild();
            boolean isPredicateTable = childSais.equals(predicates);
            for (int i=0, arrayLen=childArray.length(); i < arrayLen; ++i) {
                JSONObject childJson = childArray.getJSONObject(i);
                validateFKs(childJson, childFK, atTablePK, scratch);
                boolean sawPredicatesByNow = isPredicateTable | sawPredicateTable;
                ret |= validateResponseRecursively(childJson, childSais, predicates, sawPredicatesByNow, scratch);
            }
        }
        return ret;
    }

    private static List<Object> getPK(JSONObject jsonObject, SaisTable atTable, StringBuilder scratch) {
        List<String> atTablePK = atTable.getPK();
        List<Object> values = new ArrayList<Object>(atTablePK.size());
        for (String pkField : atTablePK) {
            pkField = escapeField(pkField, scratch);
            try {
                Object value = jsonObject.get(pkField);
                values.add(value);
            } catch (JSONException e) {
                fail(FIELDS_MISSING, String.format("%s didn't have a value for one of its PK fields: %s", atTable, pkField));
            }
        }
        assert values.size() == atTablePK.size() : String.format("size mismatch: %s != %s", values, atTablePK);
        return values;
    }

    private static void validateFKs(JSONObject childJson, SaisFK childFK, List<Object> atTablePK, StringBuilder scratch)
            throws JSONException
    {
        if (orphansSelectFromMissingParents(atTablePK)) {
            return;
        }
        List<Object> fkFields = new ArrayList<Object>(atTablePK.size());
        Iterator<String> childFKCols = childFK.getChildCols();
        while (childFKCols.hasNext()) {
            String childFKCol = escapeField(childFKCols.next(), scratch);
            fkFields.add(childJson.get(childFKCol));
        }
        assertEquals(FK_MISMATCH, "FK fields for " + childFK, atTablePK, fkFields);
    }

    private static boolean orphansSelectFromMissingParents(List<Object> atTablePK) {
        // TODO bug 723347. If necessary, remove this method (and its use) when that bug is resolved
        for (Object o : atTablePK) {
            if (!JSONObject.NULL.equals(o)) {
                return false;
            }
        }
        return true;
    }

    private static void validateFields(String tableName,
                                       List<String> expectedFields, JSONObject jsonObject, StringBuilder scratch)
    {
        List<String> missing = null;
        for (String field : expectedFields) {
            field = escapeField(field, scratch);
            if (!jsonObject.has(field)) {
                missing = (missing == null) ? new ArrayList<String>(jsonObject.length()) : missing;
                missing.add(field);
            }
            else {
                Object o;
                try {
                    o = jsonObject.get(field);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
                assertFalse(INVALID_FIELD, "field's value is a JSON array or object",
                    (o instanceof JSONObject) || (o instanceof JSONArray)
                );
            }
            // TODO field value validation? type? anything?
        }
        if (missing != null) {
            fail(FIELDS_MISSING, "Missing fields for table " + tableName + ": " + missing);
        }
    }

    private static String escapeField(String field, StringBuilder scratch) {
        char firstChar = field.charAt(0);
        if (firstChar == ':' || firstChar == '@') {
            scratch.setLength(0);
            field = scratch.append(':').append(field).toString();
        }
        return field;
    }

    private static String escapeTable(SaisTable table, StringBuilder scratch) {
        scratch.setLength(0);
        return scratch.append('@').append(table.getName()).toString();
    }
}
