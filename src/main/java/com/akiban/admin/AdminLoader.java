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

package com.akiban.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AdminLoader
{
    public static void main(String[] args) throws IOException, InterruptedException, Admin.StaleUpdateException
    {
        new AdminLoader(args).run();
    }

    private AdminLoader(String[] args)
    {
        try {
            int a = 0;
            String configRootPath = args[a++];
            configRoot = new File(configRootPath);
            if (!(configRoot.exists() && configRoot.isDirectory())) {
                usage();
            }
        } catch (Exception e) {
            usage();
        }
    }

    private void run() throws IOException, InterruptedException, Admin.StaleUpdateException
    {
        List<File> configFiles = configFiles();
        checkConfigFiles(configFiles);
        uploadConfigFiles(configFiles);
    }

    private List<File> configFiles()
    {
        List<File> configFiles = new ArrayList<File>();
        findConfigFiles(configRoot, configFiles);
        return configFiles;
    }

    private void findConfigFiles(File directory, List<File> configFiles)
    {
        assert directory.isDirectory() : directory;
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                findConfigFiles(file, configFiles);
            } else {
                configFiles.add(file);
            }
        }
    }

    private void checkConfigFiles(List<File> configFiles)
    {
        List<File> nonConfigFiles = new ArrayList<File>(configFiles);
        // Check that every required config file is present
        for (String requiredConfigFile : AdminKey.CONFIG_KEYS) {
            boolean found = false;
            for (File configFile : configFiles) {
                if (configFile.getAbsolutePath().endsWith(requiredConfigFile)) {
                    found = true;
                    nonConfigFiles.remove(configFile);
                }
            }
            if (!found) {
                error(String.format("%s not present under %s", requiredConfigFile, configRoot));
            }
        }
/* Need to ignore at least .svn directory
        // Other files shouldn't be there
        for (File nonConfigFile : nonConfigFiles) {
            error(String.format("%s, found under %s, is not a valid config file", nonConfigFile, configRoot));
        }
*/
    }

    private void uploadConfigFiles(List<File> configFiles)
        throws IOException, InterruptedException, Admin.StaleUpdateException
    {
        Admin admin = Admin.only();
        for (String key : AdminKey.CONFIG_KEYS) {
            AdminValue value = admin.get(key);
            assert key.charAt(0) == '/';
            String relativePath = key.substring(1);
            String newValue = fileContents(new File(configRoot, relativePath));
            admin.set(key, value == null ? -1 : value.version(), newValue);
        }
    }

    private String fileContents(File file) throws IOException
    {
        StringBuilder buffer = new StringBuilder();
        BufferedReader input = new BufferedReader(new FileReader(file));
        String line;
        while ((line = input.readLine()) != null) {
            buffer.append(line);
            buffer.append('\n');
        }
        return buffer.toString();
    }

    private static void error(String message)
    {
        System.err.println(message);
        System.exit(2);
    }

    private static void usage()
    {
        for (String s : USAGE_BEFORE) {
            System.err.println(s);
        }
        for (String key : AdminKey.CONFIG_KEYS) {
            System.err.println(String.format("    %s", key));
        }
        for (String s : USAGE_AFTER) {
            System.err.println(s);
        }
        System.exit(1);
    }

    private static final String[] USAGE_BEFORE = {
        "akconfigure CONFIG_ROOT",
        "CONFIG_ROOT is a directory containing the Akiban configuration files:",

    };

    private static final String[] USAGE_AFTER = {
        "There must be no other files or directories under CONFIG_ROOT."
    };

    private File configRoot;
}
