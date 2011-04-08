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
        XPathExpression expr = xpath.compile("//model/version");
        modelVersion =  ((Double)expr.evaluate(document, XPathConstants.NUMBER)).intValue();
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
