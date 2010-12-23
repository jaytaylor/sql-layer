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
