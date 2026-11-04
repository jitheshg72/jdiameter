/*
 * TeleStax, Open Source Cloud Communications
 * Copyright 2011-2014, TeleStax Inc. and individual contributors
 * by the @authors tag.
 *
 * This program is free software: you can redistribute it and/or modify
 * under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package org.jdiameter.common.impl.transport.sctp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.nio.sctp.AbstractNotificationHandler;
import com.sun.nio.sctp.AssociationChangeNotification;
import com.sun.nio.sctp.HandlerResult;
import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.ShutdownNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single established SCTP association, backed by the JDK's {@code com.sun.nio.sctp} support (module {@code jdk.sctp}).
 * A dedicated reader thread delivers payloads and lifecycle events to a {@link Listener}.
 */
public class SctpAssociation {

  /**
   * Receives events from an association. Callbacks are invoked from the association reader thread.
   */
  public interface Listener {

    void onCommunicationUp();

    void onCommunicationShutdown();

    void onPayload(byte[] data, int streamNumber, int payloadProtocolId);
  }

  private static final Logger logger = LoggerFactory.getLogger(SctpAssociation.class);

  private static final long CONNECT_POLL_MS = 50;

  private final SctpChannel channel;
  private final String name;
  private final int bufferSize;
  private final Listener listener;
  private final AtomicBoolean shutdownNotified = new AtomicBoolean(false);
  private volatile boolean connected = false;
  private volatile boolean closed = false;

  public SctpAssociation(SctpChannel channel, String name, int bufferSize, Listener listener) {
    this.channel = channel;
    this.name = name;
    this.bufferSize = bufferSize;
    this.listener = listener;
  }

  /**
   * Opens a channel bound to the given local addresses (the first is the primary, the rest are for multi-homing) and connects it to
   * the remote address, waiting at most {@code timeoutMs}.
   */
  public static SctpChannel connect(InetSocketAddress local, String[] extraLocalAddresses, InetSocketAddress remote, long timeoutMs)
      throws IOException {
    final SctpChannel channel = SctpChannel.open();
    boolean success = false;
    try {
      channel.bind(local);
      if (extraLocalAddresses != null) {
        for (String extra : extraLocalAddresses) {
          channel.bindAddress(InetAddress.getByName(extra));
        }
      }
      channel.configureBlocking(false);
      boolean done = channel.connect(remote);
      final long deadline = System.currentTimeMillis() + timeoutMs;
      while (!done) {
        if (System.currentTimeMillis() > deadline) {
          throw new IOException("Timed out connecting SCTP association to " + remote);
        }
        try {
          Thread.sleep(CONNECT_POLL_MS);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while connecting SCTP association to " + remote, e);
        }
        done = channel.finishConnect();
      }
      channel.configureBlocking(true);
      success = true;
      return channel;
    }
    finally {
      if (!success) {
        closeQuietly(channel);
      }
    }
  }

  /**
   * Starts the reader thread. The association is considered up from this point.
   */
  public void start() {
    connected = true;
    listener.onCommunicationUp();
    final Thread reader = new Thread(new Runnable() {
      @Override
      public void run() {
        readLoop();
      }
    }, "jdiameter-sctp-" + name);
    reader.setDaemon(true);
    reader.start();
  }

  public boolean isConnected() {
    return connected && !closed;
  }

  public void send(byte[] data, int streamNumber, int payloadProtocolId) throws IOException {
    if (!isConnected()) {
      throw new IOException("SCTP association '" + name + "' is not connected");
    }
    final MessageInfo info = MessageInfo.createOutgoing(null, streamNumber);
    info.payloadProtocolID(payloadProtocolId);
    info.complete(true);
    info.unordered(false);
    channel.send(ByteBuffer.wrap(data), info);
  }

  public void close() {
    closed = true;
    connected = false;
    closeQuietly(channel);
  }

  private void readLoop() {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
    final TerminationHandler handler = new TerminationHandler();
    ByteArrayOutputStream partial = null;
    try {
      while (!closed) {
        buffer.clear();
        final MessageInfo info = channel.receive(buffer, null, handler);
        if (info == null || handler.terminated) {
          logger.debug("SCTP association '{}' ended (info={}, notification={})", name, info, handler.terminated);
          break;
        }
        buffer.flip();
        if (buffer.remaining() == 0 && info.isComplete() && partial == null) {
          continue;
        }
        final byte[] chunk = new byte[buffer.remaining()];
        buffer.get(chunk);
        if (!info.isComplete()) {
          if (partial == null) {
            partial = new ByteArrayOutputStream();
          }
          partial.write(chunk, 0, chunk.length);
          continue;
        }
        byte[] data = chunk;
        if (partial != null) {
          partial.write(chunk, 0, chunk.length);
          data = partial.toByteArray();
          partial = null;
        }
        deliver(data, info.streamNumber(), info.payloadProtocolID());
      }
    }
    catch (Exception e) {
      if (!closed) {
        logger.debug("SCTP association '{}' reader terminated", name, e);
      }
    }
    finally {
      logger.debug("SCTP association '{}' reader exiting (closedLocally={})", name, closed);
      connected = false;
      closed = true;
      closeQuietly(channel);
      notifyShutdown();
    }
  }

  private void deliver(byte[] data, int streamNumber, int payloadProtocolId) {
    try {
      listener.onPayload(data, streamNumber, payloadProtocolId);
    }
    catch (RuntimeException e) {
      logger.error("Listener failed processing payload on SCTP association '{}'", name, e);
    }
  }

  private void notifyShutdown() {
    if (shutdownNotified.compareAndSet(false, true)) {
      try {
        listener.onCommunicationShutdown();
      }
      catch (RuntimeException e) {
        logger.debug("Listener failed handling shutdown of SCTP association '{}'", name, e);
      }
    }
  }

  private static void closeQuietly(SctpChannel channel) {
    try {
      channel.close();
    }
    catch (IOException e) {
      logger.trace("Error closing SCTP channel", e);
    }
  }

  private static class TerminationHandler extends AbstractNotificationHandler<Object> {

    private volatile boolean terminated = false;

    @Override
    public HandlerResult handleNotification(AssociationChangeNotification notification, Object attachment) {
      switch (notification.event()) {
        case COMM_LOST:
        case SHUTDOWN:
          terminated = true;
          return HandlerResult.RETURN;
        default:
          return HandlerResult.CONTINUE;
      }
    }

    @Override
    public HandlerResult handleNotification(ShutdownNotification notification, Object attachment) {
      terminated = true;
      return HandlerResult.RETURN;
    }
  }
}
