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

 package com.akiban.server;

 import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
  
  
public class AkServerJmxManage {
  

    public static int jmxport;
    public static String jmxhost;
    private static String jmxurl; 

    private static JMXServiceURL url 		;
    private static JMXConnector jmxc 		;
    private static MBeanServerConnection mbsc 	;
    private static ObjectName mbean 		;

    public static void initJmx() throws Exception
    {
    	    if (jmxport==0) jmxport=Integer.parseInt(System.getProperty("jmxport"));
	    if (jmxhost==null) jmxhost= System.getProperty("jmxhost");
	    System.out.println ( "Using jmxhost=" + jmxhost + ", Using jmxport=" + jmxport);
    	    if (jmxurl==null) jmxurl = "service:jmx:rmi:///jndi/rmi://"+ jmxhost+":"+jmxport+"/jmxrmi";
	    System.out.println ( "Using jmxurl=" + jmxurl);
	    if (url==null)   url = new JMXServiceURL(jmxurl);
	    if (jmxc==null)  jmxc = JMXConnectorFactory.connect(url, null);
	    if (mbsc==null)  mbsc = jmxc.getMBeanServerConnection();
    	    if (mbean==null) mbean = new ObjectName("com.akiban:type=AKSERVER");

    }
 
    public static String arrayToString(String[] a, String separator) {
	    StringBuffer result = new StringBuffer();
	    if (a.length > 0) {
		//        	result.append(a[0]);
	        for (int i=1; i<a.length; i++) {
        	    result.append(separator);
	            result.append(a[i]);
        	}
	    }
    	return result.toString();
    }  




    public static void main(String[] args)  
    {
	try{

 	    initJmx();    

	    if (args.length == 0  )
	    {
        	throw new Exception ("methodName must be the first argument");
	    }
  
	    String method = args[0];
	    int argcnt = args.length-1;
 
	    Object[] params = new Object[argcnt];
	    String[] signature = new String[argcnt];
	    for (int i = 1; i < args.length; i++ )
	    {
	
    		params[i-1]    = new String (args[i]);
	    	signature[i-1] = new String ("java.lang.String");
	    }
	
 	    Object retObj = mbsc.invoke(mbean, method, params,signature);
 	    System.out.println ( "JMX call " + method + "(" + arrayToString(args, ",")+ ")" + " returned : " + retObj);
	} catch (Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
     }
}

