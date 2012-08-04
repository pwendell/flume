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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * A {@link LineReader} which reads log data from files stored in a
 * spooling directory and renames each file once all of its data has been
 * read (through {@link #readLine()} calls).
 *
 * This reader assumes that files with unique file names are left in the
 * spooling directory and not modified once they are placed their. Any user
 * behavior which violates these assumptions, when detected, will result in a
 * FlumeException being thrown.
 *
 * This class makes the following guarantees, if above assumptions are met:
 *   1) Once a log file has been renamed with the {@link #COMPLETED_SUFFIX},
 *      all of its records have been read through the {@link #readLine()}
 *      function at least once.
 *   2) All log files in the spooling directory will eventually be opened
 *      and delivered to {@link #readLine()} a caller.
 *
 * Note that once this has reached the final line of a file, it will roll the
 * file on the *subsequent* call to {@link #readLine()} or {@link #readLines()}.
 * This lets the caller ensure that a set of lines are transported before
 * calling this again, facilitating a lightweight transaction-ish mechanism.
 */
public class SpoolingFileLineReader implements LineReader {
  public static String COMPLETED_SUFFIX = ".COMPLETE";
  private static final Logger logger = LoggerFactory
      .getLogger(SpoolingFileLineReader.class);
  private static Long POLL_SLEEP_MS = 500L;

  private File directory;
  private FileInfo currentFile;

  public SpoolingFileLineReader(File directory) {
    Preconditions.checkNotNull(directory);
    if (!directory.isDirectory()) {
      throw new FlumeException("File is not a directory: " +
        directory.getAbsolutePath());
    }
    this.directory = directory;
    currentFile = getNextFile();
  }

  @Override
  public String readLine() throws IOException {
    String out = currentFile.getReader().readLine();
    while (out == null) {
      retireCurrentFile();
      out = currentFile.getReader().readLine();
    }
    return out;
  }


  @Override
  /** Reads up to n lines from the current file. Will not read beyond a file
   *  boundary. */
  public List<String> readLines(int n) throws IOException {
    List<String> out = Lists.newLinkedList();
    String outLine = currentFile.getReader().readLine();
    if (outLine == null) {
      retireCurrentFile();
      return readLines(n);
    }
    while (outLine != null) {
      out.add(outLine);
      if (out.size() == n) { break; }
      outLine = currentFile.getReader().readLine();
    }
    return out;
  }

  /**
   * Closes currentFile, attempt to rename it, and load a new file.
   *
   * If these operations fail in a way that may cause duplicate log entries,
   * an error is logged but no exceptions are thrown. If these operations fail
   * in a way that indicates potential misuse of the spooling directory, a
   * FlumeException will be thrown.
   */
  private void retireCurrentFile() throws IOException {
    String currPath = currentFile.getFile().getAbsolutePath();
    String newPath = currPath + COMPLETED_SUFFIX;
    logger.info("Moving ingested file " + currPath + " to " + newPath);

    File newFile = new File(currPath);

    // Verify that spooling assumptions hold
    if (newFile.lastModified() != currentFile.getLastModified()) {
      String message = "File has been modified since being read: " + currPath;
      logger.error(message);
      throw new FlumeException(message);
    }
    if (newFile.length() != currentFile.getLength()) {
      String message = "File has changed size since being read: " + currPath;
      logger.error(message);
      throw new FlumeException(message);
    }

    // Before renaming, check whether destination file name exists
    File existing = new File(newPath);
    if (existing.exists()) {
      /*
       * This is a tricky situation. We are trying to move the log file to
       * indicate completion, but there is already a completed file.
       * There are at least two distinct cases where this could happen:
       *
       * 1) The client previously completed this file, but the rename was
       *    not atomic and both old and new copies exist.
       *
       * 2) The user is not actually using unique filenames and the completed
       *    file is colliding with another file.
       *
       * If we are in case 1 we want to keep going like normal, if we are in
       * case 2 we throw an exception to the user since it indicates
       * improper use of the spooling client.
       */
      if (Files.equal(currentFile.getFile(), existing)) {
        logger.warn("Completed file " + newPath +
            " already exists, but files match, so continuing.");
        boolean deleted = newFile.delete();
        if (!deleted) {
          logger.error("Unable to delete file " + newFile.getAbsolutePath() +
              ". It will likely be ingested another time.");
        }
      }
      else {
        throw new FlumeException("File name has been re-used with different" +
          "sized files. Spooling assumption violated for " + newPath);
      }
    } else { // Dest file does not already exist
      boolean renamed = newFile.renameTo(new File(newPath));
      if (!renamed) {
        /* If we are here then the file cannot be renamed for a reason other
         * than that the destination file exists (actually, that remains
         * possible w/ small probability due to TOC-TOU conditions).
         *
         * The most likely scenario is that we don't have permissions to create
         * the new file or remove the old one, so we say that in the message.*/
        logger.error("Unable to move {} to {}. This will likely cause " +
        		"duplicate events. Please verify that flume has sufficient " +
        		"permissions to perform these operations", currPath, newPath);
      }
    }
    currentFile.reader.close();
    currentFile = getNextFile();
  }

  @Override
  public void close() throws IOException {
    currentFile.getReader().close();
  }

  /**
   * Chose and open a random file in the chosen directory, if the directory is
   * empty, this will block until a new file has been found.
   */
  private FileInfo getNextFile() {
    List<File> candidateFiles = Lists.newArrayList();

    while (true) {
      File[] children = directory.listFiles();
      for (File child: children) {
        if (!child.getName().endsWith(COMPLETED_SUFFIX)) {
          candidateFiles.add(child);
        }
      }
      if (!candidateFiles.isEmpty()) {
        try {
          BufferedReader reader = new BufferedReader(
              new FileReader(candidateFiles.get(0)));
          return new FileInfo(candidateFiles.get(0), reader);
        } catch (FileNotFoundException e) {
          // File could have been deleted in the interim, if so just wait
          // until next loop iteration
        }
      }
      else {
        try {
          Thread.sleep(POLL_SLEEP_MS);
        } catch (InterruptedException e) {
          logger.error("Unexpected interruption", e);
          System.exit(-1);
        }
      }
    }
  }

  /** An immutable class with information about a file being processed. */
  private class FileInfo {
    private File file;
    private long length;
    private long lastModified;
    private BufferedReader reader;

    public FileInfo(File file, BufferedReader reader) {
      this.file = file;
      this.length = file.length();
      this.lastModified = file.lastModified();
      this.reader = reader;
    }

    public long getLength() { return this.length; }
    public long getLastModified() { return this.lastModified; }
    public BufferedReader getReader() { return this.reader; }
    public File getFile() { return this.file; }
  }
}
