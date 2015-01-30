package com.lucidworks.spark.streaming.netty;

import com.lucidworks.spark.streaming.MessageHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

import java.io.OutputStreamWriter;

public class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {

  private final MessageHandler handler;

  private OutputStreamWriter osw;

  HttpClientHandler(MessageHandler handler) {
    this.handler = handler;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
    if (msg instanceof HttpResponse) {
      HttpResponse response = (HttpResponse) msg;

      System.out.println("STATUS: " + response.getStatus());
      System.out.println("VERSION: " + response.getProtocolVersion());
      System.out.println();

      if (!response.headers().isEmpty()) {
        for (String name : response.headers().names()) {
          for (String value : response.headers().getAll(name)) {
            System.out.println("HEADER: " + name + " = " + value);
          }
        }
        System.out.println();
      }

      if (HttpHeaders.isTransferEncodingChunked(response)) {
        System.out.println("CHUNKED CONTENT {");
      } else {
        System.out.println("CONTENT {");
      }
    }
    if (msg instanceof HttpContent) {
      HttpContent content = (HttpContent) msg;

      //System.out.print(content.content().toString(CharsetUtil.UTF_8));
      //System.out.flush();

      String messageContent = content.content().toString(CharsetUtil.UTF_8);
      if (messageContent != null) {
        handler.handleMessage(messageContent);

        //osw.write(messageContent.trim());
        //osw.write('\n');
        //osw.flush();
      }

      if (content instanceof LastHttpContent) {
        //System.out.println("} END OF CONTENT");

        //buffer.setLength(0);
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }
}

