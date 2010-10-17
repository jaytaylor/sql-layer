package com.akiban.ais.util;

import com.akiban.ais.model.AkibaInformationSchema;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;

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
