/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExtensionsLoaderTest
{
  @TempDir
  public File temporaryFolder;

  private Injector startupInjector()
  {
    return new StartupInjectorBuilder()
        .withEmptyProperties()
        .withExtensions()
        .build();
  }

  @Test
  public void test02MakeStartupInjector()
  {
    Injector startupInjector = startupInjector();
    Assertions.assertNotNull(startupInjector);
    Assertions.assertNotNull(startupInjector.getInstance(ObjectMapper.class));
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);
    Assertions.assertNotNull(extnLoader);
    Assertions.assertSame(extnLoader, ExtensionsLoader.instance(startupInjector));
  }

  @Test
  public void test04DuplicateClassLoaderExtensions() throws Exception
  {
    final File extensionDir = newFolder(temporaryFolder, "junit");
    Injector startupInjector = startupInjector();
    ExtensionsLoader extnLoader = ExtensionsLoader.instance(startupInjector);

    Pair<File, Boolean> key = Pair.of(extensionDir, true);
    extnLoader.getLoadersMap()
                  .put(key, new URLClassLoader(new URL[]{}, ExtensionsLoader.class.getClassLoader()));

    Collection<DruidModule> modules = extnLoader.getFromExtensions(DruidModule.class);

    Set<String> loadedModuleNames = new HashSet<>();
    for (DruidModule module : modules) {
      Assertions.assertFalse(loadedModuleNames.contains(module.getClass().getName()), "Duplicate extensions are loaded");
      loadedModuleNames.add(module.getClass().getName());
    }
  }

  @Test
  public void test06GetClassLoaderForExtension() throws IOException
  {
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig());

    final File some_extension_dir = newFolder(temporaryFolder, "junit");
    final File a_jar = new File(some_extension_dir, "a.jar");
    final File b_jar = new File(some_extension_dir, "b.jar");
    final File c_jar = new File(some_extension_dir, "c.jar");
    a_jar.createNewFile();
    b_jar.createNewFile();
    c_jar.createNewFile();
    final URLClassLoader loader = extnLoader.getClassLoaderForExtension(some_extension_dir, false);
    final URL[] expectedURLs = new URL[]{a_jar.toURI().toURL(), b_jar.toURI().toURL(), c_jar.toURI().toURL()};
    final URL[] actualURLs = loader.getURLs();
    Arrays.sort(actualURLs, Comparator.comparing(URL::getPath));
    Assertions.assertArrayEquals(expectedURLs, actualURLs);
  }

  @Test
  public void testGetLoadedModules()
  {
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig());
    Collection<DruidModule> modules = extnLoader.getModules();
    HashSet<DruidModule> moduleSet = new HashSet<>(modules);

    Collection<DruidModule> loadedModules = extnLoader.getModules();
    Assertions.assertEquals(modules.size(), loadedModules.size(), "Set from loaded modules #1 should be same!");
    Assertions.assertEquals(moduleSet, new HashSet<>(loadedModules), "Set from loaded modules #1 should be same!");

    Collection<DruidModule> loadedModules2 = extnLoader.getModules();
    Assertions.assertEquals(modules.size(), loadedModules2.size(), "Set from loaded modules #2 should be same!");
    Assertions.assertEquals(moduleSet, new HashSet<>(loadedModules2), "Set from loaded modules #2 should be same!");
  }

  @Test
  public void testGetExtensionFilesToLoad_non_exist_extensions_dir() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "junit");
    Assertions.assertTrue(!tmpDir.exists() || tmpDir.delete(), "could not create missing folder");
    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return tmpDir.getAbsolutePath();
      }
    });
    Assertions.assertArrayEquals(
        new File[]{},
        extnLoader.getExtensionFilesToLoad(),
        "Non-exist root extensionsDir should return an empty array of File"
    );
  }


  @Test
  public void testGetExtensionFilesToLoad_wrong_type_extensions_dir() throws IOException
  {
    assertThrows(ISE.class, () -> {
      final File extensionsDir = File.createTempFile("junit", null, temporaryFolder);
      final ExtensionsConfig config = new ExtensionsConfig()
      {
        @Override
        public String getDirectory()
        {
          return extensionsDir.getAbsolutePath();
        }
      };
      final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
      extnLoader.getExtensionFilesToLoad();
    });
  }

  @Test
  public void testGetExtensionFilesToLoad_empty_extensions_dir() throws IOException
  {
    final File extensionsDir = newFolder(temporaryFolder, "junit");
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };

    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    Assertions.assertArrayEquals(
        new File[]{},
        extnLoader.getExtensionFilesToLoad(),
        "Empty root extensionsDir should return an empty array of File"
    );
  }

  /**
   * If druid.extension.load is not specified, Initialization.getExtensionFilesToLoad is supposed to return all the
   * extension folders under root extensions directory.
   */
  @Test
  public void testGetExtensionFilesToLoad_null_load_list() throws IOException
  {
    final File extensionsDir = newFolder(temporaryFolder, "junit");
    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    mysql_metadata_storage.mkdir();

    final File[] expectedFileList = new File[]{mysql_metadata_storage};
    final File[] actualFileList = extnLoader.getExtensionFilesToLoad();
    Arrays.sort(actualFileList);
    Assertions.assertArrayEquals(expectedFileList, actualFileList);
  }

  /**
   * druid.extension.load is specified, Initialization.getExtensionFilesToLoad is supposed to return all the extension
   * folders appeared in the load list.
   */
  @Test
  public void testGetExtensionFilesToLoad_with_load_list() throws IOException
  {
    final File extensionsDir = newFolder(temporaryFolder, "junit");

    final File absolutePathExtension = newFolder(temporaryFolder, "junit");

    final ExtensionsConfig config = new ExtensionsConfig()
    {
      @Override
      public LinkedHashSet<String> getLoadList()
      {
        return Sets.newLinkedHashSet(Arrays.asList("mysql-metadata-storage", absolutePathExtension.getAbsolutePath()));
      }

      @Override
      public String getDirectory()
      {
        return extensionsDir.getAbsolutePath();
      }
    };
    final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
    final File mysql_metadata_storage = new File(extensionsDir, "mysql-metadata-storage");
    final File random_extension = new File(extensionsDir, "random-extensions");

    mysql_metadata_storage.mkdir();
    random_extension.mkdir();

    final File[] expectedFileList = new File[]{mysql_metadata_storage, absolutePathExtension};
    final File[] actualFileList = extnLoader.getExtensionFilesToLoad();
    Assertions.assertArrayEquals(expectedFileList, actualFileList);
  }

  /**
   * druid.extension.load is specified, but contains an extension that is not prepared under root extension directory.
   * Initialization.getExtensionFilesToLoad is supposed to throw ISE.
   */
  @Test
  public void testGetExtensionFilesToLoad_with_non_exist_item_in_load_list() throws IOException
  {
    assertThrows(ISE.class, () -> {
      final File extensionsDir = newFolder(temporaryFolder, "junit");
      final ExtensionsConfig config = new ExtensionsConfig()
      {
        @Override
        public LinkedHashSet<String> getLoadList()
        {
          return Sets.newLinkedHashSet(ImmutableList.of("mysql-metadata-storage"));
        }

        @Override
        public String getDirectory()
        {
          return extensionsDir.getAbsolutePath();
        }
      };
      final File random_extension = new File(extensionsDir, "random-extensions");
      random_extension.mkdir();
      final ExtensionsLoader extnLoader = new ExtensionsLoader(config);
      extnLoader.getExtensionFilesToLoad();
    });
  }

  @Test
  public void testGetURLsForClasspath() throws Exception
  {
    File tmpDir1 = newFolder(temporaryFolder, "junit");
    File tmpDir2 = newFolder(temporaryFolder, "junit");
    File tmpDir3 = newFolder(temporaryFolder, "junit");

    File tmpDir1a = new File(tmpDir1, "a.jar");
    tmpDir1a.createNewFile();
    File tmpDir1b = new File(tmpDir1, "b.jar");
    tmpDir1b.createNewFile();
    new File(tmpDir1, "note1.txt").createNewFile();

    File tmpDir2c = new File(tmpDir2, "c.jar");
    tmpDir2c.createNewFile();
    File tmpDir2d = new File(tmpDir2, "d.jar");
    tmpDir2d.createNewFile();
    File tmpDir2e = new File(tmpDir2, "e.JAR");
    tmpDir2e.createNewFile();
    new File(tmpDir2, "note2.txt").createNewFile();

    String cp = tmpDir1.getAbsolutePath() + File.separator + "*"
                + File.pathSeparator
                + tmpDir3.getAbsolutePath()
                + File.pathSeparator
                + tmpDir2.getAbsolutePath() + File.separator + "*";

    // getURLsForClasspath uses listFiles which does NOT guarantee any ordering for the name strings.
    List<URL> urLsForClasspath = ExtensionsLoader.getURLsForClasspath(cp);
    Assertions.assertEquals(Sets.newHashSet(tmpDir1a.toURI().toURL(), tmpDir1b.toURI().toURL()),
                        Sets.newHashSet(urLsForClasspath.subList(0, 2)));
    Assertions.assertEquals(tmpDir3.toURI().toURL(), urLsForClasspath.get(2));
    Assertions.assertEquals(Sets.newHashSet(tmpDir2c.toURI().toURL(), tmpDir2d.toURI().toURL(), tmpDir2e.toURI().toURL()),
                        Sets.newHashSet(urLsForClasspath.subList(3, 6)));
  }

  @Test
  public void testExtensionsWithSameDirName() throws Exception
  {
    final String extensionName = "some_extension";
    final File tmpDir1 = newFolder(temporaryFolder, "junit");
    final File tmpDir2 = newFolder(temporaryFolder, "junit");
    final File extension1 = new File(tmpDir1, extensionName);
    final File extension2 = new File(tmpDir2, extensionName);
    Assertions.assertTrue(extension1.mkdir());
    Assertions.assertTrue(extension2.mkdir());
    final File jar1 = new File(extension1, "jar1.jar");
    final File jar2 = new File(extension2, "jar2.jar");

    Assertions.assertTrue(jar1.createNewFile());
    Assertions.assertTrue(jar2.createNewFile());

    final ExtensionsLoader extnLoader = new ExtensionsLoader(new ExtensionsConfig());
    final ClassLoader classLoader1 = extnLoader.getClassLoaderForExtension(extension1, false);
    final ClassLoader classLoader2 = extnLoader.getClassLoaderForExtension(extension2, false);

    Assertions.assertArrayEquals(new URL[]{jar1.toURI().toURL()}, ((URLClassLoader) classLoader1).getURLs());
    Assertions.assertArrayEquals(new URL[]{jar2.toURI().toURL()}, ((URLClassLoader) classLoader2).getURLs());
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
