/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** See src/main/resources/version/fdbsql_version.properties */
public class LayerVersionInfo
{
    private static final Logger LOG = LoggerFactory.getLogger(LayerVersionInfo.class);
    private static final String UNKNOWN_DEFAULT = "UNKNOWN";

    public LayerVersionInfo(Properties props) {
        this.version = getOrWarn(props, "version");
        this.versionShort = this.version.replace("-SNAPSHOT", "");

        String[] nums = versionShort.split("\\.");
        int major = -1, minor = -1, patch = -1, release = -1;
        try {
            major = Integer.parseInt(nums[0]);
            minor = Integer.parseInt(nums[1]);
            patch = Integer.parseInt(nums[2]);
            release = Integer.parseInt(getOrWarn(props, "release"));
        }
        catch (Exception e) {
            LOG.warn("Couldn't parse version number: " + versionShort);
        }
        this.versionMajor = major;
        this.versionMinor = minor;
        this.versionPatch = patch;
        this.versionRelease = release;
        this.versionIsSnapshot = !this.version.equals(this.versionShort);

        this.buildTime = getOrWarn(props, "build_time");

        this.gitBranch = getOrWarn(props, "git_branch");
        this.gitCommitTime = getOrWarn(props, "git_commit_time");
        this.gitDescribe = getOrWarn(props, "git_describe");
        this.gitHash = getOrWarn(props, "git_hash");
        this.gitHashShort = this.gitHash.substring(0, 7);

        this.versionLong = this.version + "+" + this.gitHashShort;
    }

    private static String getOrWarn(Properties props, String propName) {
        String value = props.getProperty(propName);
        if(value == null) {
            LOG.warn("Version property '{}' not found", propName);
            return UNKNOWN_DEFAULT;
        }
        return value;
    }


    /** As in pom: x.y.z[-SNAPSHOT] */
    public final String version;
    /** As in pom, no snapshot: x.y.z */
    public final String versionShort;
    /** As in pom, with short hash: x.y.z[-SNAPSHOT]+shortHash */
    public final String versionLong;

    public final int versionMajor;
    public final int versionMinor;
    public final int versionPatch;
    public final int versionRelease;
    public final boolean versionIsSnapshot;

    public final String buildTime;

    public final String gitBranch;
    public final String gitCommitTime;
    public final String gitDescribe;
    public final String gitHash;
    public final String gitHashShort;
}
