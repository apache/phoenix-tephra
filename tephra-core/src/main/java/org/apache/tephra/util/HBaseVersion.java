/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects the currently loaded HBase version.  It is assumed that only one HBase version is loaded at a time,
 * since using more than one HBase version within the same process will require classloader isolation anyway.
 */
public class HBaseVersion {
  private static final String HBASE_94_VERSION = "0.94";
  private static final String HBASE_96_VERSION = "0.96";
  private static final String HBASE_98_VERSION = "0.98";
  private static final String HBASE_10_VERSION = "1.0";
  private static final String HBASE_11_VERSION = "1.1";
  private static final String HBASE_12_VERSION = "1.2";
  private static final String HBASE_13_VERSION = "1.3";
  private static final String HBASE_14_VERSION = "1.4";
  private static final String HBASE_15_VERSION = "1.5";
  private static final String HBASE_16_VERSION = "1.6";
  private static final String HBASE_20_VERSION = "2.0";
  private static final String HBASE_21_VERSION = "2.1";
  private static final String HBASE_22_VERSION = "2.2";
  private static final String HBASE_23_VERSION = "2.3";
  private static final String HBASE_24_VERSION = "2.4";
  private static final String CDH_CLASSIFIER = "cdh";

  private static final Logger LOG = LoggerFactory.getLogger(HBaseVersion.class);

  /**
   * Represents the major version of the HBase library that is currently loaded.
   */
  public enum Version {
    HBASE_94("0.94"),
    HBASE_96("0.96"),
    HBASE_98("0.98"),
    HBASE_10("1.0"),
    HBASE_10_CDH("1.0-cdh"),
    HBASE_11("1.1"),
    HBASE_12("1.2"),
    HBASE_13("1.3"),
    HBASE_14("1.4"),
    HBASE_15("1.5"),
    HBASE_16("1.6"),
    HBASE_20("2.0"),
    HBASE_21("2.1"),
    HBASE_22("2.2"),
    HBASE_23("2.3"),
    HBASE_24("2.4"),
    UNKNOWN("unknown");

    final String majorVersion;

    Version(String majorVersion) {
      this.majorVersion = majorVersion;
    }

    public String getMajorVersion() {
      return majorVersion;
    }
  }

  private static Version currentVersion;
  private static String versionString;
  static {
    try {
      Class versionInfoClass = Class.forName("org.apache.hadoop.hbase.util.VersionInfo");
      Method versionMethod = versionInfoClass.getMethod("getVersion");
      versionString = (String) versionMethod.invoke(null);
      if (versionString.startsWith(HBASE_94_VERSION)) {
        currentVersion = Version.HBASE_94;
      } else if (versionString.startsWith(HBASE_96_VERSION)) {
        currentVersion = Version.HBASE_96;
      } else if (versionString.startsWith(HBASE_98_VERSION)) {
        currentVersion = Version.HBASE_98;
      } else if (versionString.startsWith(HBASE_10_VERSION)) {
        VersionNumber ver = VersionNumber.create(versionString);
        if (ver.getClassifier() != null && ver.getClassifier().startsWith(CDH_CLASSIFIER)) {
          currentVersion = Version.HBASE_10_CDH;
        } else {
          currentVersion = Version.HBASE_10;
        }
      } else if (versionString.startsWith(HBASE_11_VERSION)) {
        currentVersion = Version.HBASE_11;
      } else if (versionString.startsWith(HBASE_12_VERSION)) {
        currentVersion = Version.HBASE_12;
      } else if (versionString.startsWith(HBASE_13_VERSION)) {
        currentVersion = Version.HBASE_13;
      } else if (versionString.startsWith(HBASE_14_VERSION)) {
        currentVersion = Version.HBASE_14;
      } else if (versionString.startsWith(HBASE_15_VERSION)) {
        currentVersion = Version.HBASE_15;
      } else if (versionString.startsWith(HBASE_16_VERSION)) {
          currentVersion = Version.HBASE_16;
      } else if (versionString.startsWith(HBASE_20_VERSION)) {
          currentVersion = Version.HBASE_20;
      } else if (versionString.startsWith(HBASE_21_VERSION)) {
          currentVersion = Version.HBASE_21;
      } else if (versionString.startsWith(HBASE_22_VERSION)) {
          currentVersion = Version.HBASE_22;
      } else if (versionString.startsWith(HBASE_23_VERSION)) {
          currentVersion = Version.HBASE_23;
      } else if (versionString.startsWith(HBASE_24_VERSION)) {
        currentVersion = Version.HBASE_24;
      } else {
        currentVersion = Version.UNKNOWN;
      }
    } catch (Throwable e) {
      // must be a class loading exception, HBase is not there
      LOG.error("Unable to determine HBase version from string '{}', are HBase classes available?", versionString);
      LOG.error("Exception was: ", e);
      currentVersion = Version.UNKNOWN;
    }
  }

  /**
   * Returns the major version of the currently loaded HBase library.
   */
  public static Version get() {
    return currentVersion;
  }

  /**
   * Returns the full version string for the currently loaded HBase library.
   */
  public static String getVersionString() {
    return versionString;
  }

  /**
   * Prints out the HBase {@link Version} enum value for the current version of HBase on the classpath.
   */
  public static void main(String[] args) {
    boolean verbose = args.length == 1 && "-v".equals(args[0]);
    Version version = HBaseVersion.get();
    System.out.println(version.getMajorVersion());
    if (verbose) {
      System.out.println("versionString=" + getVersionString());
    }
  }

  /**
   * Utility class to parse apart version number components.  The version string provided is expected to be in
   * the format: major[.minor[.patch[.last]][-classifier][-SNAPSHOT]
   *
   * <p>Only the major version number is actually required.</p>
   */
  public static class VersionNumber {
    private static final Pattern PATTERN =
        Pattern.compile("(\\d+)(\\.(\\d+))?(\\.(\\d+))?(\\.(\\d+))?(\\-(?!SNAPSHOT)([^\\-]+))?(\\-SNAPSHOT)?");

    private Integer major;
    private Integer minor;
    private Integer patch;
    private Integer last;
    private String classifier;
    private boolean snapshot;

    private VersionNumber(Integer major, Integer minor, Integer patch, Integer last,
                          String classifier, boolean snapshot) {
      this.major = major;
      this.minor = minor;
      this.patch = patch;
      this.last = last;
      this.classifier = classifier;
      this.snapshot = snapshot;
    }

    public Integer getMajor() {
      return major;
    }

    public Integer getMinor() {
      return minor;
    }

    public Integer getPatch() {
      return patch;
    }

    public Integer getLast() {
      return last;
    }

    public String getClassifier() {
      return classifier;
    }

    public boolean isSnapshot() {
      return snapshot;
    }

    public static VersionNumber create(String versionString) throws ParseException {
      Matcher matcher = PATTERN.matcher(versionString);
      if (matcher.matches()) {
        String majorString = matcher.group(1);
        String minorString = matcher.group(3);
        String patchString = matcher.group(5);
        String last = matcher.group(7);
        String classifier = matcher.group(9);
        String snapshotString = matcher.group(10);
        return new VersionNumber(new Integer(majorString),
            minorString != null ? new Integer(minorString) : null,
            patchString != null ? new Integer(patchString) : null,
            last != null ? new Integer(last) : null,
            classifier,
            "-SNAPSHOT".equals(snapshotString));
      }
      throw new ParseException(
          "Input string did not match expected pattern: major[.minor[.patch]][-classifier][-SNAPSHOT]", 0);
    }
  }
}
