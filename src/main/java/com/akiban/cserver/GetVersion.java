package com.akiban.cserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetVersion{

    public static void main (String[] a) throws Exception
    {
        System.out.println (new GetVersion("/akserver_version") );
    }

    String version;

    public GetVersion(String jarResource) throws IOException {
        InputStream in = getClass().getResourceAsStream( jarResource );
        BufferedReader reader = new BufferedReader ( new InputStreamReader ( in ) );

        version = reader.readLine();

        reader.close();
        in.close();
    }
    @Override
        public String toString(){
            return   version.toString();  
        }
}

