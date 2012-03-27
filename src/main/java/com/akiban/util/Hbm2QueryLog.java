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

package com.akiban.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Primitive parser to extract queries from uTest hibernate files and create an
 * Akiban-formatted properties file.
 * 
 * @author peter
 * 
 */
public class Hbm2QueryLog {

    private static class Query {
        String name;
        String sql;

        private Query(final String name, final String sql) {
            this.name = name;
            this.sql = sql;
        }
    }

    private List<Query> extractQueries(final Document document)
            throws Exception {
        final List<Query> queries = new ArrayList<Query>();
        final XPath xpath = XPathFactory.newInstance().newXPath();
        final XPathExpression expr = xpath
                .compile("//hibernate-mapping/sql-query");
        final NodeList queryNodeList = (NodeList) expr.evaluate(document,
                XPathConstants.NODESET);
        for (int i = 0; i < queryNodeList.getLength(); i++) {
            final Node node = queryNodeList.item(i);
            // object tags have only one attribute - name
            final String name = node.getAttributes().getNamedItem("name")
                    .getTextContent();
            final StringBuilder sb = new StringBuilder();
            Node child = node.getFirstChild();
            while (child != null) {
                sb.append(child.getTextContent());
                child = child.getNextSibling();
            }
            queries.add(new Query(name, sb.toString()));
        }
        return queries;
    }

    public static void main(final String[] args) throws Exception {

        final Hbm2QueryLog parser = new Hbm2QueryLog();

        for (int i = 0; i < args.length; i++) {
            final DocumentBuilder documentBuilder = DocumentBuilderFactory
                    .newInstance().newDocumentBuilder();
            final InputStream objectModelInputStream = new FileInputStream(
                    args[i]);
            final List<Query> results = parser.extractQueries(documentBuilder
                    .parse(objectModelInputStream));

            System.out.println();
            System.out.println("# " + args[i]);
            for (final Query query : results) {
                System.out.println(query.name + "= \\");
                String[] pieces = query.sql.split("\\n");
                for (int k = 0; k < pieces.length; k++) {
                    System.out.println(pieces[k] + " \\");
                }
                System.out.println();
            }
            System.out.println();
        }
    }
}
