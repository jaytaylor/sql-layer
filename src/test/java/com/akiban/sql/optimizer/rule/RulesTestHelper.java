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

package com.akiban.sql.optimizer.rule;

import com.akiban.ais.model.AkibanInformationSchema;

import com.akiban.server.rowdata.SchemaFactory;

import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;

public class RulesTestHelper
{
    private RulesTestHelper() {
    }

    public static List<BaseRule> loadRules(File file) throws Exception {
        Yaml yaml = new Yaml();
        FileInputStream istr = new FileInputStream(file);
        List<Object> list = (List<Object>)yaml.load(istr);
        istr.close();
        return parseRules(list);
    }

    public static List<BaseRule> parseRules(String str) throws Exception {
        Yaml yaml = new Yaml();
        List<Object> list = (List<Object>)yaml.load(str);
        return parseRules(list);
    }

    public static List<BaseRule> parseRules(List<Object> list) throws Exception {
        List<BaseRule> result = new ArrayList<BaseRule>();
        for (Object obj : list) {
            if (obj instanceof String) {
                String cname = (String)obj;
                if (cname.indexOf('.') < 0)
                    cname = RulesTestHelper.class.getPackage().getName() + '.' + cname;
                result.add((BaseRule)Class.forName(cname).newInstance());
            }
            else {
                // TODO: Someday parse options from hash, etc.
                throw new Exception("Don't know what to do with " + obj);
            }
        }
        return result;
    }

    // Make fake row def cache to keep UserTableRowType constructor
    // and Index.getAllColumns() from getting NPE.
    public static void ensureRowDefs(AkibanInformationSchema ais) {
        new SchemaFactory().buildRowDefs(ais);
    }
}
