/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.client.avro;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.flume.FlumeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestSpoolingFileLineReader {
  private File tmpDir;

  @Before
  public void before() {
    tmpDir = Files.createTempDir();
  }

  @After
  public void after() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test
  /** Create three multi-line files then read them back out. */
  public void testBasicSpooling() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    File f3 = new File(tmpDir.getAbsolutePath() + "/file3");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);
    Files.write("file3line1\nfile3line2\n", f3, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir);

    List<String> out = Lists.newArrayList();
    for (int i = 0; i < 6; i++) {
      out.add(reader.readLine());
    }
    // Make sure we got every line
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));
    assertTrue(out.contains("file3line1"));
    assertTrue(out.contains("file3line2"));

    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());

    assertEquals(3, outFiles.size());

    // Make sure each file (or its postprocessing equivalent) exists
    assertTrue(outFiles.contains(new File(tmpDir + "/file1")) ||
        outFiles.contains(new File(tmpDir + "/file1.COMPLETE")));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")) ||
        outFiles.contains(new File(tmpDir + "/file2.COMPLETE")));
    assertTrue(outFiles.contains(new File(tmpDir + "/file3")) ||
        outFiles.contains(new File(tmpDir + "/file3.COMPLETE")));

    /* Exactly one of the files should still be open
      (which one is nondeterministic).*/
    int numIncomplete = 0;
    if (outFiles.contains(new File(tmpDir + "/file1"))) {
      numIncomplete++;
    }
    if (outFiles.contains(new File(tmpDir + "/file2"))) {
      numIncomplete++;
    }
    if (outFiles.contains(new File(tmpDir + "/file3"))) {
      numIncomplete++;
    }

    assertEquals(1, numIncomplete);
  }

  @Test(expected = FlumeException.class)
  /** Ensures that file immutability is enforced. */
  public void testFileChangesDuringRead() throws IOException {
    File tmpDir1 = Files.createTempDir();
    File f1 = new File(tmpDir1.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);

    SpoolingFileLineReader reader1 = new SpoolingFileLineReader(tmpDir1);

    List<String> out = Lists.newArrayList();
    out.add(reader1.readLine());
    out.add(reader1.readLine());

    assertEquals(2, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));

    Files.append("file1line3\n", f1, Charsets.UTF_8);

    out.add(reader1.readLine());
    out.add(reader1.readLine());
  }


  /** Test when a competing destination file is found, but it matches. */
  @Test
  public void testDestinationExistsAndSameFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
      SpoolingFileLineReader.COMPLETED_SUFFIX);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1line2\n", f1Completed, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir);

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
    }

    // Make sure we got every line
    assertEquals(4, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
    assertTrue(outFiles.contains(new File(tmpDir + "/file1" +
        SpoolingFileLineReader.COMPLETED_SUFFIX)));
  }

  /** Test when a competing destination file is found and it does not match. */
  @Test(expected = FlumeException.class)
  public void testDestinationExistsAndDifferentFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    File f1Completed = new File(tmpDir.getAbsolutePath() + "/file1" +
      SpoolingFileLineReader.COMPLETED_SUFFIX);

    Files.write("file1line1\nfile1line2\n", f1, Charsets.UTF_8);
    Files.write("file1line1\nfile1XXXe2\n", f1Completed, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir);

    List<String> out = Lists.newArrayList();

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
    }

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\n", f2, Charsets.UTF_8);

    for (int i = 0; i < 2; i++) {
      out.add(reader.readLine());
    }

    // Make sure we got every line
    assertEquals(4, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file2line1"));
    assertTrue(out.contains("file2line2"));

    // Make sure original is deleted
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
    assertTrue(outFiles.contains(new File(tmpDir + "/file1" +
        SpoolingFileLineReader.COMPLETED_SUFFIX)));
  }

  @Test
  public void testBatchedReadsWithinAFile() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir);

    List<String> out = reader.readLines(5);

    // Make sure we got every line
    assertEquals(5, out.size());
    assertTrue(out.contains("file1line1"));
    assertTrue(out.contains("file1line2"));
    assertTrue(out.contains("file1line3"));
    assertTrue(out.contains("file1line4"));
    assertTrue(out.contains("file1line5"));
  }

  @Test
  public void testBatchedReadsAcrossFileBoundary() throws IOException {
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    SpoolingFileLineReader reader = new SpoolingFileLineReader(tmpDir);
    List<String> out1 = reader.readLines(5);

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write("file2line1\nfile2line2\nfile2line3\nfile2line4\n" +
        "file2line5\nfile2line6\nfile2line7\nfile2line8\n",
        f2, Charsets.UTF_8);

    List<String> out2 = reader.readLines(5);
    List<String> out3 = reader.readLines(5);

    // Should have first 5 lines of file1
    assertEquals(5, out1.size());
    assertTrue(out1.contains("file1line1"));
    assertTrue(out1.contains("file1line2"));
    assertTrue(out1.contains("file1line3"));
    assertTrue(out1.contains("file1line4"));
    assertTrue(out1.contains("file1line5"));

    // Should have 3 remaining lines of file1
    assertEquals(3, out2.size());
    assertTrue(out2.contains("file1line6"));
    assertTrue(out2.contains("file1line7"));
    assertTrue(out2.contains("file1line8"));

    // Should have first 5 lines of file2
    assertEquals(5, out3.size());
    assertTrue(out3.contains("file2line1"));
    assertTrue(out3.contains("file2line2"));
    assertTrue(out3.contains("file2line3"));
    assertTrue(out3.contains("file2line4"));
    assertTrue(out3.contains("file2line5"));

    // file1 should be moved now
    List<File> outFiles = Lists.newArrayList(tmpDir.listFiles());
    assertEquals(2, outFiles.size());
    assertTrue(outFiles.contains(new File(tmpDir + "/file1" +
        SpoolingFileLineReader.COMPLETED_SUFFIX)));
    assertTrue(outFiles.contains(new File(tmpDir + "/file2")));
  }
}
