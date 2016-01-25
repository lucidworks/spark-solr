package com.lw.spark.streaming;

public interface MessageHandler {
    void handleMessage(String msg);
    void onEndOfStream();
}
