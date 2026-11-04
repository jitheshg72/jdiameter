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

package org.jdiameter.server.impl.io.sctp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

import org.jdiameter.api.AvpDataException;
import org.jdiameter.client.api.io.NotInitializedException;
import org.jdiameter.client.impl.transport.sctp.SCTPTransportClient;
import org.jdiameter.common.impl.transport.sctp.SctpAssociation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCTP server transport based on the JDK SCTP support ({@code com.sun.nio.sctp}). The listening instance accepts incoming
 * associations; each accepted association gets its own instance, wrapped by a {@link SCTPServerConnection}.
 *
 * @author <a href="mailto:brainslog@gmail.com"> Alexandre Mendonca </a>
 * @author <a href="mailto:baranowb@gmail.com"> Bartosz Baranowski </a>
 */
public class SCTPTransportServer {

  private static final int ACCEPT_BACKLOG = 10;

  private volatile SctpServerChannel serverChannel = null;
  private volatile Thread acceptThread = null;
  private volatile boolean accepting = false;
  private volatile SctpAssociation remoteClientAssociation = null;
  private SCTPServerConnection parentConnection;
  private String serverName;
  protected InetSocketAddress destAddress;
  protected InetSocketAddress origAddress;
  protected String[] extraHostAddresses;
  private static final Logger logger = LoggerFactory.getLogger(SCTPTransportServer.class);
  private volatile int payloadProtocolId = 0;
  private volatile int streamNumber = 0;

  public SCTPTransportServer() {
  }

  /**
   * Default constructor
   *
   * @param parentConnectionConnection
   *          connection created this transport
   */
  SCTPTransportServer(SCTPServerConnection parentConnectionConnection) {
    this.parentConnection = parentConnectionConnection;
  }

  public SCTPServerConnection getParent() {
    return parentConnection;
  }

  public void startNewRemoteConnection(SctpChannel channel, String peerAddress, int peerPort) {
    logger.debug("Initializing new Remote Connection '{}' -> '{}' ---> '{}:{}'", new Object[]{this.origAddress, this.destAddress, peerAddress, peerPort});
    final String remoteClientAssociationName = peerAddress + ":" + peerPort;
    final int bufferSize = Integer.parseInt(System.getProperty(
        SCTPTransportClient.SCTP_BUFFER_SIZE_PARAMETER, SCTPTransportClient.SCTP_BUFFER_SIZE_DEFAULT));

    final SctpAssociation association = new SctpAssociation(channel, remoteClientAssociationName, bufferSize, new ServerAssociationListener());
    remoteClientAssociation = association;
    association.start();
    logger.info("Connected to {}", remoteClientAssociationName);
  }

  public void startServer() throws NotInitializedException {
    if (getParent() == null) {
      throw new NotInitializedException("No parent connection is set is set");
    }

    logger.debug("Initializing SCTP server");
    logger.debug("Orig Address: '{}:{}'", origAddress.getAddress().getHostAddress(), origAddress.getPort());
    logger.debug("Dest Address: '{}'", this.destAddress);
    serverName = origAddress.getHostName() + ":" + origAddress.getPort();

    SctpServerChannel channel = null;
    try {
      channel = SctpServerChannel.open();
      channel.bind(new InetSocketAddress(origAddress.getAddress(), origAddress.getPort()), ACCEPT_BACKLOG);
      channel.configureBlocking(false);
      if (extraHostAddresses != null) {
        for (String extra : extraHostAddresses) {
          channel.bindAddress(InetAddress.getByName(extra));
        }
      }
    }
    catch (IOException e) {
      closeQuietly(channel);
      throw new NotInitializedException("Failed to start SCTP server on " + serverName, e);
    }

    this.serverChannel = channel;
    final SctpServerChannel acceptChannel = channel;
    final Thread acceptor = new Thread(new Runnable() {
      @Override
      public void run() {
        acceptLoop(acceptChannel);
      }
    }, "jdiameter-sctp-accept-" + serverName);
    acceptor.setDaemon(true);
    this.acceptThread = acceptor;
    this.accepting = true;
    acceptor.start();

    logger.debug("Successfuly initialized SCTP Server Host[{}]", serverName);
  }

  // the accepting thread owns the listening channel and closes it on exit so the port is released deterministically
  private void acceptLoop(SctpServerChannel channel) {
    Selector selector = null;
    try {
      selector = Selector.open();
      channel.register(selector, SelectionKey.OP_ACCEPT);
      while (accepting && channel.isOpen()) {
        selector.select(200);
        selector.selectedKeys().clear();
        SctpChannel accepted = null;
        try {
          accepted = channel.accept();
          if (accepted != null) {
            accepted.configureBlocking(true);
            logger.debug("Received notfification of a new remote connection!");
            getParent().onNewRemoteConnection(accepted);
          }
        }
        catch (IOException e) {
          logger.error("Error accepting SCTP connection", e);
          closeQuietly(accepted);
        }
        catch (Exception e) {
          logger.error("Failed to handle new SCTP remote connection", e);
          closeQuietly(accepted);
        }
      }
    }
    catch (IOException e) {
      logger.error("SCTP accept loop failed", e);
    }
    finally {
      closeQuietly(selector);
      closeQuietly(channel);
    }
  }

  private class ServerAssociationListener implements SctpAssociation.Listener {

    private final Logger logger = LoggerFactory.getLogger(ServerAssociationListener.class);

    @Override
    public void onCommunicationUp() {
      logger.debug("onCommunicationUp called for [{}]", serverName);
      getParent().onConnected();
    }

    @Override
    public void onCommunicationShutdown() {
      logger.debug("onCommunicationShutdown called for [{}]", serverName);
      try {
        getParent().onDisconnect();
      }
      catch (Exception e) {
        logger.debug("Error", e);
      }
    }

    @Override
    public void onPayload(byte[] data, int stream, int ppid) {
      // set payload and stream number values;
      payloadProtocolId = ppid;
      streamNumber = stream;

      logger.debug("SCTP Server received a message of length: [{}] ", data.length);

      try {
        // make a message out of data and process it
        getParent().onMessageReceived(ByteBuffer.wrap(data));
      }
      catch (AvpDataException e) {
        logger.debug("Garbage was received. Discarding.");
        getParent().onAvpDataException(e);
      }
    }
  }

  public void destroy() throws Exception {
    logger.debug("Destroying SCTP Server");
    this.stop();
    this.accepting = false;
    final Thread acceptor = this.acceptThread;
    if (acceptor != null) {
      this.acceptThread = null;
      acceptor.join(5000);
    }
    final SctpServerChannel channel = this.serverChannel;
    if (channel != null) {
      this.serverChannel = null;
      closeQuietly(channel);
    }
  }

  public void stop() throws Exception {
    logger.debug("Stopping SCTP Server");

    // Note we never stop the listening channel here as it is always listening - we only stop the remote client association
    final SctpAssociation association = this.remoteClientAssociation;
    if (association != null) {
      association.close();
    }
  }

  public void release() throws Exception {
    logger.debug("Releasing SCTP Server");

    // Note we never release the listening channel here - we only stop the remote client association
    this.stop();
    remoteClientAssociation = null;
  }

  public InetSocketAddress getDestAddress() {
    return this.destAddress;
  }

  public void setDestAddress(InetSocketAddress address) {
    this.destAddress = address;
    if (logger.isDebugEnabled()) {
      logger.debug("Destination address is set to [{}:{}]", destAddress.getHostName(), destAddress.getPort());
    }
  }

  public void setOrigAddress(InetSocketAddress address) {
    this.origAddress = address;
    if (logger.isDebugEnabled()) {
      logger.debug("Origin address is set to [{}:{}]", origAddress.getHostName(), origAddress.getPort());
    }
  }

  public void setExtraHostAddresses(String[] extraHostAddresses) {
    this.extraHostAddresses = extraHostAddresses;
    if (logger.isDebugEnabled() && extraHostAddresses != null) {
      for(final String address : extraHostAddresses) {
        logger.debug("Extra host address is set to [{}]", address);
      }
    }
  }

  public InetSocketAddress getOrigAddress() {
    return this.origAddress;
  }

  public void sendMessage(ByteBuffer bytes) throws IOException {
    final SctpAssociation association = this.remoteClientAssociation;
    if (association == null) {
      throw new IOException("SCTP server connection has no remote association");
    }
    if (logger.isDebugEnabled()) {
      logger.debug("About to send a byte buffer of size [{}] over the SCTP", bytes.array().length);
    }

    association.send(bytes.array(), streamNumber, payloadProtocolId);

    if (logger.isDebugEnabled()) {
      logger.debug("Sent a byte buffer of size [{}] over SCTP", bytes.array().length);
    }
  }

  boolean isConnected() {
    final SctpAssociation association = this.remoteClientAssociation;
    return association != null && association.isConnected();
  }

  private static void closeQuietly(java.io.Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      }
      catch (IOException e) {
        logger.trace("Error closing SCTP channel", e);
      }
    }
  }
}
