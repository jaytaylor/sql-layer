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

package com.akiban.server.mttests.mthapi.common;

import com.akiban.server.api.HapiGetRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

final class JsonUtils {
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
}
