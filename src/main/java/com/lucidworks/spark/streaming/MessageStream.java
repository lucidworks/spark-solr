package com.lucidworks.spark.streaming;

import java.io.Serializable;
import java.net.URI;

public interface MessageStream extends Serializable {
    URI getURI();
    void receive(MessageHandler handler);
    void stopReceiving();
}
