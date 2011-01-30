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

package com.akiban.ais.util;

import java.io.StringWriter;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.akiban.ais.model.AkibaInformationSchema;

public class AISTextGenerator
{
    public String generate(VelocityContext context, String templateName) throws Exception
    {
        VelocityContext contextWithAIS = new VelocityContext(context);
        contextWithAIS.put("ais", ais);
        Template template = velocity.getTemplate(templateName);
        StringWriter stringWriter = new StringWriter();
        template.merge(contextWithAIS, stringWriter);
        return stringWriter.toString();
    }

    public AISTextGenerator(AkibaInformationSchema ais) throws Exception
    {
        this.ais = ais;
        velocity = new VelocityEngine();
        velocity.setProperty("resource.loader",
                             "class");
        velocity.setProperty("class.resource.loader.class",
                             "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
        velocity.setProperty("directive.foreach.counter.initial.value",
                             "0");        
        velocity.init();
    }

    private final AkibaInformationSchema ais;
    private final VelocityEngine velocity;
}
