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

import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.junit.Test;

import static com.akiban.server.test.mt.mthapi.common.HapiValidationError.*;

public class JsonUtilsTest {
    @Test
    public void validCOIA() throws JSONException {
        SaisTable cTable;
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").pk("iid").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        cTable = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@c").array()
                        .object()
                            .key("cid").value(1)
                            .key("@a").array()
                                .object()
                                    .key("aid").value(1)
                                    .key("c_id").value(1)
                                .endObject()
                            .endArray()
                            .key("@o").array()
                                .object()
                                    .key("oid").value(1)
                                    .key("c_id").value(1)
                                    .key("@i").array().endArray()
                                .endObject()
                                .object()
                                    .key("oid").value(1)
                                    .key("c_id").value(1)
                                    .key("@i").array()
                                        .object()
                                            .key("iid").value(1)
                                            .key("o_id").value(1)
                                        .endObject()
                                    .endArray()
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        // from root
        JsonUtils.validateResponse(json, cTable, cTable);
    }

    @Test
    public void rootHasNoPK() throws JSONException {
        SaisTable one = new SaisBuilder().table("one", "id").backToBuilder().getSoleRootTable();
        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@one").array()
                        .object()
                            .key("id").value(1)
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one);
    }

    @Test
    public void multiColumnFK() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("parent", "id1", "id2").pk("id1", "id2");
        builder.table("child", "id", "pid1", "pid2").joinTo("parent").col("id1", "pid1").col("id2", "pid2");
        SaisTable parent = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@parent").array()
                        .object()
                            .key("id1").value(11)
                            .key("id2").value(22)
                            .key("@child").array()
                                .object()
                                    .key("id").value(1)
                                    .key("pid1").value(11)
                                    .key("pid2").value(22)
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, parent, parent);
    }

    @Test
    public void orphanRowResurrectsParent() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("parent", "id").pk("id");
        builder.table("child", "id", "pid").joinTo("parent").col("id", "pid");
        SaisTable parent = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@parent").array()
                        .object()
                            .key("id").value(null)
                            .key("@child").array()
                                .object()
                                    .key("id").value(1)
                                    .key("pid").value(11)
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, parent, parent);
    }

    @Test
    public void orphanRowResurrectsParentMultiColumn() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("parent", "id1", "id2").pk("id1", "id2");
        builder.table("child", "id", "pid1", "pid2").joinTo("parent").col("id1", "pid1").col("id2", "pid2");
        SaisTable parent = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@parent").array()
                        .object()
                            .key("id1").value(null)
                            .key("id2").value(null)
                            .key("@child").array()
                                .object()
                                    .key("id").value(1)
                                    .key("pid1").value(11)
                                    .key("pid2").value(22)
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, parent, parent);
    }

    @Test(expected=FkMismatchError.class)
    public void orphanRowResurrectsParentMultiColumnPartially() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("parent", "id1", "id2").pk("id1", "id2");
        builder.table("child", "id", "pid1", "pid2").joinTo("parent").col("id1", "pid1").col("id2", "pid2");
        SaisTable parent = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@parent").array()
                        .object()
                            .key("id1").value(null)
                            .key("id2").value(22)
                            .key("@child").array()
                                .object()
                                    .key("id").value(1)
                                    .key("pid1").value(11)
                                    .key("pid2").value(22)
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, parent, parent);
    }

    @Test(expected=RootTablesCountError.class)
    public void tooManyRoots() throws JSONException {
        SaisTable one = new SaisBuilder().table("one", "id", "hello").backToBuilder().getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                        .key("@one").array().endArray()
                        .key("@two").array().endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one);
    }

    @Test(expected=UnseenPredicatesError.class)
    public void tooManyChildren() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "id").pk("id");
        builder.table("o", "id", "c_id").pk("id").joinTo("c").col("id", "c_id");
        builder.table("i", "id", "o_id").pk("id").joinTo("o").col("id", "o_id");
        SaisTable c = builder.getSoleRootTable();
        SaisTable i = c.getChild("o").getChild("i");

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                        .key("@c").array()
                            .object()
                                .key("id").value(1)
                                .key("@o").array()
                                    // just having two objects here will trigger the error
                                    .object().endObject()
                                    .object().endObject()
                                .endArray()
                            .endObject()
                        .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, c, i);
    }

    @Test(expected=RootTableNameError.class)
    public void rootNotFound() throws JSONException {
        SaisTable one = new SaisBuilder().table("one", "id", "hello").backToBuilder().getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                        .key("@two").array().endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one);
    }

    @Test(expected=UnseenPredicatesError.class)
    public void predicateNotFound() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("one", "id", "hello").pk("id");
        builder.table("child", "id", "oneid").joinTo("one").col("id", "oneid");
        SaisTable one = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                        .key("@one").array()
                            .object()
                                .key("id").value(1)
                                .key("hello").value("world")
                            .endObject()
                        .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one.getChild("child"));
    }

    @Test(expected=FieldsMissingError.class)
    public void missingField() throws JSONException {
        SaisTable zebra = new SaisBuilder().table("zebra", "id", "stripes").backToBuilder().getSoleRootTable();
        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@zebra").array()
                        .object()
                            .key("id").value(1)
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, zebra, zebra);
    }

    @Test(expected=InvalidFieldError.class)
    public void fieldIsArray() throws JSONException {
        SaisTable one = new SaisBuilder().table("one", "id").pk("id").backToBuilder().getSoleRootTable();
        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@one").array()
                        .object()
                            .key("id").array().endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one);
    }

    @Test(expected=InvalidFieldError.class)
    public void fieldIsObject() throws JSONException {
        SaisTable one = new SaisBuilder().table("one", "id").backToBuilder().getSoleRootTable();
        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@one").array()
                        .object()
                            .key("id").object().endObject()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, one, one);
    }

    @Test(expected=FkMismatchError.class)
    public void fkMismatchInt() throws JSONException {
        SaisBuilder builder = new SaisBuilder();
        builder.table("parent", "id").pk("id");
        builder.table("child", "id", "pid").joinTo("parent").col("id", "pid");
        SaisTable parent = builder.getSoleRootTable();

        JSONObject json = new JSONObject(
                new JSONStringer()
                .object()
                    .key("@parent").array()
                        .object()
                            .key("id").value(1)
                            .key("@child").array()
                                .object()
                                    .key("id").value(1)
                                    .key("pid").value(2)
                                .endObject()
                            .endArray()
                        .endObject()
                    .endArray()
                .endObject()
                .toString()
        );
        JsonUtils.validateResponse(json, parent, parent);
    }

    @Test(expected=ResponseIsNullError.class)
    public void nullResponse() throws JSONException {
        JsonUtils.validateResponse(null, null, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullSelectTable() {
        SaisTable table = new SaisBuilder().table("hi").backToBuilder().getSoleRootTable();
        JsonUtils.validateResponse(new JSONObject(), null, table);
    }

    @Test(expected=IllegalArgumentException.class)
    public void nullPredicatesTable() {
        SaisTable table = new SaisBuilder().table("hi").backToBuilder().getSoleRootTable();
        JsonUtils.validateResponse(new JSONObject(), table, null);
    }
}
