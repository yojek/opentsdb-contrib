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

package net.opentsdb.proxy.netty;

import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

public class ConnectionManager extends SimpleChannelHandler {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
  private static final DefaultChannelGroup channels = new DefaultChannelGroup("all-channels");

  static void closeAllConnections() {
    channels.close().awaitUninterruptibly();
  }

  /**
   * Constructor.
   */
  public ConnectionManager() {
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) {
    channels.add(e.getChannel());
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      logger.info(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
    final Throwable cause = e.getCause();
    final Channel chan = ctx.getChannel();
    if (cause instanceof ClosedChannelException) {
      logger.warn("Attempt to write to closed channel " + chan);
      return;
    }
    if (cause instanceof IOException) {
      final String message = cause.getMessage();
      if ("Connection reset by peer".equals(message)
          || "Connection timed out".equals(message)) {
        // Do nothing. A client disconnecting isn't really our problem. Oh,
        // and I'm not kidding you, there's no better way to detect ECONNRESET
        // in Java. Like, people have been bitching about errno for years,
        // and Java managed to do something *far* worse. That's quite a feat.
        return;
      }
    }
    logger.error("Unexpected exception from downstream for " + chan, cause);
    e.getChannel().close();
  }

}