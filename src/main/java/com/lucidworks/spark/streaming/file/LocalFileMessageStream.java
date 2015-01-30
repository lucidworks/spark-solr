package com.lucidworks.spark.streaming.file;

import com.lucidworks.spark.streaming.MessageHandler;
import com.lucidworks.spark.streaming.MessageStream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;

public class LocalFileMessageStream implements MessageStream {

  public String path;
  private transient FileReaderThread fileReaderThread;

  public LocalFileMessageStream() {

  }

  public LocalFileMessageStream(String localFilePath) {
    setPath(localFilePath);
  }

  public void setPath(String path) {
    File verifyFile = new File(path);
    if (!verifyFile.isFile()) {
      throw new IllegalArgumentException("File " + path + "not found!");
    }
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public URI getURI() {
    return new File(path).toURI();
  }

  public void receive(MessageHandler handler) {
    // must produce messages in the background
    fileReaderThread = new FileReaderThread(handler);
    fileReaderThread.start();
  }

  public void stopReceiving() {
    if (fileReaderThread != null) {
      try {
        fileReaderThread.interrupt();
      } catch (Exception exc) {}
    }
  }

  private class FileReaderThread extends Thread {
    MessageHandler handler;

    private FileReaderThread(MessageHandler handler) {
      this.handler = handler;
    }

    public void run() {
      String line;
      int lineNum = 0;
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        while ((line = reader.readLine()) != null) {
          ++lineNum;
          line = line.trim();
          if (line.length() > 0) {
            handler.handleMessage(line);
          }
        }
      } catch (Exception exc) {
        if (!(exc instanceof InterruptedException)) {
          throw new RuntimeException(exc);
        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception ignore) {}
        }
      }

      handler.onEndOfStream();
    }
  }
}
