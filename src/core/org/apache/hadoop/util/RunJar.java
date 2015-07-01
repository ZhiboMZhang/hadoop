/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/** Run a Hadoop job jar. */
public class RunJar {

  private static final Log LOG = LogFactory.getLog(RunJar.class);

  /** Unpack a jar file into a directory. */
  public static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /** Run a Hadoop job jar.  If the main class is not in the jar's manifest,
   * then it must be provided on the command line. */
  public static void main(String[] args) throws Throwable {
    String usage = "RunJar jarFile [mainClass [args...]]";
    LOG.info("Got args: " + Arrays.toString(args));

    // Jar file name is required.
    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int currentArg = 0;
    boolean jobInHdfs = false;
    String fileName = args[currentArg++];

    // Check to see if the jar file is in hdfs. If so copy it to the local
    // filesystem so that it can be unjarred and run. Then remove it after.
    Configuration configuration = new Configuration();
    FileSystem hdfs = FileSystem.get(configuration);
    FileSystem localfs = FileSystem.getLocal(configuration);

    Path filePath = new Path(fileName);
    if (hdfs.exists(filePath)) {
      LOG.info("Jar is located in HDFS at " + filePath);

      // Get the jar name from the path.
      String[] tokens = fileName.split(Path.SEPARATOR);
      String newFileName = tokens[tokens.length - 1];

      LOG.info("Copying jar to local file " + newFileName);
      hdfs.copyToLocalFile(filePath, new Path(newFileName));

      fileName = newFileName;
      jobInHdfs = true;
    }

    File file = new File(fileName);
    if (!file.exists() || !file.isFile()) {
      System.err.println("Not a valid JAR: " + file.getCanonicalPath());
      System.exit(-1);
    }

    // Try to get any properties set in the manifest.
    String mainClassName = null;
    String arguments = null;
    String inputDirectory = null;
    String outputDirectory = null;

    JarFile jarFile;
    try {
      jarFile = new JarFile(fileName);
      Manifest manifest = jarFile.getManifest();

      if (manifest != null) {
        // Class-Name attribute may be set even if others aren't.
        try {
          mainClassName = manifest.getMainAttributes().getValue("Main-Class");
        } catch (IllegalArgumentException ia) {}
        try {
          arguments = manifest.getMainAttributes().getValue("Arguments");
          inputDirectory = manifest.getMainAttributes().getValue("Input-Directory");
          outputDirectory = manifest.getMainAttributes().getValue("Output-Directory");
        } catch (IllegalArgumentException ia) {}
      }
    } catch (IOException io) {
      throw new IOException("Error opening job jar: " + fileName).initCause(io);
    }
    jarFile.close();

    // Check if a main class-name was supplied.
    if (mainClassName == null) {
      if (args.length < 2) {
        System.err.println(usage);
        System.exit(-1);
      }
      mainClassName = args[currentArg++];
    }
    mainClassName = mainClassName.replaceAll("/", ".");

    // Unjar files to build up a valid classpath.
    File tmpDir = new File(new Configuration().get("hadoop.tmp.dir"));
    tmpDir.mkdirs();
    if (!tmpDir.isDirectory()) { 
      System.err.println("Mkdirs failed to create " + tmpDir);
      System.exit(-1);
    }
    final File workDir = File.createTempFile("hadoop-unjar", "", tmpDir);
    workDir.delete();
    workDir.mkdirs();
    if (!workDir.isDirectory()) {
      System.err.println("Mkdirs failed to create " + workDir);
      System.exit(-1);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          FileUtil.fullyDelete(workDir);
        } catch (IOException e) {}
      }
    });

    unJar(file, workDir);

    ArrayList<URL> classPath = new ArrayList<URL>();
    classPath.add(new File(workDir + "/").toURI().toURL());
    classPath.add(file.toURI().toURL());
    classPath.add(new File(workDir, "classes/").toURI().toURL());
    File[] libs = new File(workDir, "lib").listFiles();
    if (libs != null) {
      for (int i = 0; i < libs.length; i++) {
        classPath.add(libs[i].toURI().toURL());
      }
    }
    
    ClassLoader loader = new URLClassLoader(classPath.toArray(new URL[0]));

    // Set up and execute the jar file.
    Thread.currentThread().setContextClassLoader(loader);
    Class<?> mainClass = Class.forName(mainClassName, true, loader);
    Method main = mainClass.getMethod("main", new Class[] {
      Array.newInstance(String.class, 0).getClass()
    });
    // Main-class and classpath are properly set, just consider args + io.
    List<String> newArgsList = new ArrayList<String>();
    String[] newArgs = null;
    if (args.length < 3 && inputDirectory != null && outputDirectory != null) {
      newArgsList.addAll(Arrays.asList(inputDirectory, outputDirectory));
      if (!arguments.equals("")) {
        newArgsList.addAll(Arrays.asList(arguments.split(" ")));
      }
    } else {
      newArgsList.addAll(Arrays.asList(args).subList(currentArg, args.length));
      // Remove the main class from the arguments if it's also in the manifest.
      if (mainClassName != null && newArgsList.contains(mainClassName)) {
        newArgsList.remove(mainClassName);
      }
    }
    newArgs = newArgsList.toArray(new String[0]);

    try {
      System.out.println("Running " + mainClass.getCanonicalName()
          + " with args: " + Arrays.toString(newArgs));
      LOG.info("Running " + mainClass.getCanonicalName() + " with args: "
          + Arrays.toString(newArgs));
      main.invoke(null, new Object[] { newArgs });

      // Delete the job jar file if it was copied from HDFS.
      if (jobInHdfs) {
        Path localPath = localfs.makeQualified(new Path(fileName));
        localfs.delete(localPath, true);
        LOG.info("Jar file at " + localPath + " was deleted.");
      }

    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }
}