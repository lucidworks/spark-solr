package com.lucidworks.spark.streaming.netty;

import com.lucidworks.spark.streaming.MessageHandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;

public class HttpClientInitializer extends ChannelInitializer<SocketChannel> {

  private final MessageHandler handler;

  public HttpClientInitializer(boolean ssl, MessageHandler handler) {
    this.handler = handler;
  }

  @Override
  public void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    p.addLast("codec", new HttpClientCodec());
    p.addLast("inflater", new HttpContentDecompressor());
    p.addLast("handler", new HttpClientHandler(handler));
  }
}
