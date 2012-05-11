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

package com.akiban.ais.metamodel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class MetaModel
{
    public synchronized static MetaModel only()
    {
        if (only == null) {
            try {
            only = new MetaModel();
            } catch (Exception ex) {
                throw new MetaModelException (ex);
            }
        }
        return only;
    }

    public ModelObject definition(String objectName)
    {
        return modelObjects.get(objectName);
    }
    public final int getModelVersion() 
    {
        return modelVersion;
    }
    
    public Collection<ModelObject> getModelObjects() {
    	return Collections.unmodifiableCollection(modelObjects.values());
    }

    private MetaModel() throws ParserConfigurationException, IOException, SAXException, XPathExpressionException
    {
        document = readMetaModelSpecification();
        getVersion();
        analyze();
    }

    private void getVersion() throws XPathExpressionException
    {
        XPath xpath = XPathFactory.newInstance().newXPath();
        XPathExpression expr = xpath.compile("//model");
        Node modelNode = (Node) expr.evaluate(document, XPathConstants.NODE); 
        modelVersion = Integer.valueOf(attributeValue(modelNode.getAttributes(), "version"));
        //modelVersion =  ((Double)expr.evaluate(document, XPathConstants.NUMBER)).intValue();
    }
    
    private void analyze() throws XPathExpressionException
    {
        XPath xpath = XPathFactory.newInstance().newXPath();
        XPathExpression expr = xpath.compile("//model/object");
        NodeList modelObjectNodes = (NodeList) expr.evaluate(document, XPathConstants.NODESET);
        for (int i = 0; i < modelObjectNodes.getLength(); i++) {
            Node modelObjectNode = modelObjectNodes.item(i);
            // object tags have only one attribute - name
            String modelObjectName = modelObjectNode.getAttributes().item(0).getNodeValue();
            String tableName = modelObjectNode.getAttributes().item(1).getNodeValue();
            ModelObject modelObject = new ModelObject(modelObjectName);
            modelObject.tableName(tableName);
            // nested elements: attr, sql
            NodeList children = modelObjectNode.getChildNodes();
            for (int j = 0; j < children.getLength(); j++) {
                Node child = children.item(j);
                String elementType = child.getNodeName();
                if (elementType.equals("attr")) {
                    String name = attributeValue(child.getAttributes(), "name");
                    String type = attributeValue(child.getAttributes(), "type");
                    modelObject.addAttribute(name, type);
                } else if (elementType.equals("sql")) {
                    String action = attributeValue(child.getAttributes(), "action");
                    String query = child.getTextContent();
                    if (action.equals("read")) {
                        modelObject.readQuery(query);
                    } else if (action.equals("write")) {
                        modelObject.writeQuery(query);
                    } else if (action.equals("cleanup")) {
                        modelObject.cleanupQuery(query);
                    }
                } else if (elementType.equals("persistitTree")) {
                    String name = attributeValue(child.getAttributes(), "name");
                    modelObject.tableName(name);
                }
            }
            assert modelObject.attributes().size() > 0;
            assert modelObject.readQuery() != null;
            assert modelObject.writeQuery() != null;
            assert modelObject.cleanupQuery() != null;
            modelObjects.put(modelObject.name(), modelObject);
        }
    }

    private String attributeValue(NamedNodeMap map, String attribute)
    {
        return map.getNamedItem(attribute).getNodeValue();
    }

    private static Document readMetaModelSpecification() throws ParserConfigurationException, IOException, SAXException
    {
        DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputStream objectModelInputStream = ClassLoader.getSystemResourceAsStream(OBJECT_MODEL_FILENAME);
        if (objectModelInputStream == null)
        {
                objectModelInputStream = MetaModel.class.getClassLoader().getResourceAsStream(OBJECT_MODEL_FILENAME);
        }
        return documentBuilder.parse(objectModelInputStream);
    }

    private static final String OBJECT_MODEL_FILENAME = "ais_object_model.xml";
    private static MetaModel only;

    private final Document document;
    private final Map<String, ModelObject> modelObjects = new HashMap<String, ModelObject>();
    private int modelVersion;
}
