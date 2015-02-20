package com.lucidworks.spark.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * Adapts a MessageStream to a Spark Receiver.
 */
public class MessageStreamReceiver extends Receiver<String> implements MessageHandler {

  public static Logger log = Logger.getLogger(MessageStreamReceiver.class);

  protected MessageStream stream;

  public MessageStreamReceiver(MessageStream stream) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.stream = stream;
  }

  public void onStart() {
    log.info("Starting receiver for "+stream.getClass().getSimpleName()+": "+stream.getURI());
    stream.receive(this);
  }

  public void onStop() {
    stream.stopReceiving();
  }

  public void handleMessage(String msg) {
    String trimmed = (msg != null) ? msg.trim() : null;
    if (trimmed != null && trimmed.length() > 0) {
      if (log.isDebugEnabled())
        log.debug("Received data: "+trimmed);

      store(trimmed);
    }
  }

  public void onEndOfStream() {
    this.stop("Stream finished: "+stream.getClass().getSimpleName()+": "+stream.getURI());
  }
}
