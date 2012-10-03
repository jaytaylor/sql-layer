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

package com.akiban.server.test.it.keyupdate;

import com.akiban.ais.model.Group;
import com.akiban.server.rowdata.RowDef;

public class Schema
{
    // For all KeyUpdate*IT
    static Integer vendorId;
    static RowDef vendorRD;
    static Integer customerId;
    static RowDef customerRD;
    static Integer orderId;
    static RowDef orderRD;
    static Integer itemId;
    static RowDef itemRD;
    static Group group;
    // For KeyUpdateIT and KeyUpdateCascadingKeysIT
    static Integer v_vid;
    static Integer v_vx;
    static Integer c_cid;
    static Integer c_vid;
    static Integer c_cx;
    static Integer o_oid;
    static Integer o_cid;
    static Integer o_vid;
    static Integer o_ox;
    static Integer o_priority;
    static Integer o_when;
    static Integer i_vid;
    static Integer i_cid;
    static Integer i_oid;
    static Integer i_iid;
    static Integer i_ix;
    // For MultiColumnKeyUpdateIT and MultiColumnKeyUpdateCascadingKeysIT
    static Integer v_vid1;
    static Integer v_vid2;
    static Integer c_vid1;
    static Integer c_vid2;
    static Integer c_cid1;
    static Integer c_cid2;
    static Integer o_vid1;
    static Integer o_vid2;
    static Integer o_cid1;
    static Integer o_cid2;
    static Integer o_oid1;
    static Integer o_oid2;
    static Integer i_vid1;
    static Integer i_vid2;
    static Integer i_cid1;
    static Integer i_cid2;
    static Integer i_oid1;
    static Integer i_oid2;
    static Integer i_iid1;
    static Integer i_iid2;
}