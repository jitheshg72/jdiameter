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

package org.jdiameter.client.impl.transport.sctp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.sun.nio.sctp.SctpChannel;

import org.jdiameter.api.AvpDataException;
import org.jdiameter.client.api.io.NotInitializedException;
import org.jdiameter.common.impl.transport.sctp.SctpAssociation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SCTP client transport based on the JDK SCTP support ({@code com.sun.nio.sctp}).
 *
 * @author <a href="mailto:brainslog@gmail.com"> Alexandre Mendonca </a>
 * @author <a href="mailto:baranowb@gmail.com"> Bartosz Baranowski </a>
 */
public class SCTPTransportClient {

  // How long to wait for the association to be connected
  private static final int CONNECT_TIMEOUT = 30000;
  // The waiting time between connection attempts
  private static final int CONNECT_RETRY_DELAY = 1000;

  public static final String SCTP_BUFFER_SIZE_PARAMETER = "com.excelacom.jdiameter.sctp.bufferSize";
  public static final String SCTP_BUFFER_SIZE_DEFAULT = "65535";

  private volatile SctpAssociation clientAssociation = null;
  private SCTPClientConnection parentConnection;
  private String clientAssociationName;
  protected InetSocketAddress destAddress;
  protected InetSocketAddress origAddress;

  protected String[] extraHostAddresses;
  private int payloadProtocolId = 0;
  private int streamNumber = 0;

  private static final Logger logger = LoggerFactory.getLogger(SCTPTransportClient.class);

  public SCTPTransportClient() {
  }

  /**
   * Default constructor
   *
   * @param parenConnection
   *          connection created this transport
   */
  SCTPTransportClient(SCTPClientConnection parenConnection) {
    this.parentConnection = parenConnection;
  }

  public void initialize() throws IOException, NotInitializedException {
    logger.debug("Initializing SCTPTransportClient. Origin address is [{}] and destination address is [{}]", origAddress,
        destAddress);
    if (destAddress == null) {
      throw new NotInitializedException("Destination address is not set");
    }

    clientAssociationName = origAddress.getAddress().getHostAddress() + "." + origAddress.getPort() + "_" +
        destAddress.getAddress().getHostAddress() + "." + destAddress.getPort();
  }

  public SCTPClientConnection getParent() {
    return parentConnection;
  }

  public void start() throws NotInitializedException, IOException {
    if (getParent() == null) {
      throw new NotInitializedException("No parent connection is set");
    }
    if (clientAssociationName == null) {
      throw new NotInitializedException("Transport is not initialized");
    }

    logger.debug("Starting SCTP client. CLIENT ASSOCIATION '{}'. Origin Address [{}] <=> Dest Address [{}]", new Object[] {
        clientAssociationName, origAddress, destAddress });

    final SctpChannel channel = connectWithRetries();
    final int bufferSize = Integer.parseInt(System.getProperty(SCTP_BUFFER_SIZE_PARAMETER, SCTP_BUFFER_SIZE_DEFAULT));
    logger.debug("SCTP Client Buffer Size set to {}", bufferSize);
    final SctpAssociation association = new SctpAssociation(channel, clientAssociationName, bufferSize, new ClientAssociationListener());
    this.clientAssociation = association;
    association.start();

    logger.debug("Successfuly initialized SCTP Client Host [{}] Peer [{}]", origAddress, destAddress);
  }

  private SctpChannel connectWithRetries() throws IOException {
    final long endTStamp = System.currentTimeMillis() + CONNECT_TIMEOUT;
    IOException lastFailure = null;
    while (System.currentTimeMillis() < endTStamp) {
      try {
        return SctpAssociation.connect(origAddress, extraHostAddresses, destAddress, endTStamp - System.currentTimeMillis());
      }
      catch (IOException e) {
        lastFailure = e;
        logger.debug("SCTP connection attempt to [{}] failed, retrying", destAddress, e);
      }
      try {
        Thread.sleep(CONNECT_RETRY_DELAY);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Failed to establish SCTP connection, thread was interrupted waiting for connection.", e);
      }
    }
    throw new IOException("Failed to establish SCTP connection!", lastFailure);
  }

  private class ClientAssociationListener implements SctpAssociation.Listener {

    private final Logger logger = LoggerFactory.getLogger(ClientAssociationListener.class);

    @Override
    public void onCommunicationUp() {
      logger.debug("onCommunicationUp called for [{}]", clientAssociationName);
      getParent().onConnected();
    }

    @Override
    public void onCommunicationShutdown() {
      logger.debug("onCommunicationShutdown called for [{}]", clientAssociationName);
      try {
        getParent().onDisconnect();
      }
      catch (Exception e) {
        logger.debug("Error", e);
      }
    }

    @Override
    public void onPayload(byte[] data, int stream, int ppid) {
      logger.debug("SCTP Client received data of length [{}]", data.length);

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

  public void stop() throws Exception {
    final SctpAssociation association = this.clientAssociation;
    if (association != null) {
      association.close();
    }
  }

  public void release() throws Exception {
    this.stop();
    this.clientAssociation = null;
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

  public InetSocketAddress getOrigAddress() {
    return this.origAddress;
  }

  public void setExtraHostAddresses(String[] extraHostAddresses) {
    this.extraHostAddresses = extraHostAddresses;
    if (logger.isDebugEnabled() && extraHostAddresses != null) {
      for(final String address : extraHostAddresses) {
        logger.debug("Extra host address is set to [{}]", address);
      }
    }
  }

  public String[] getExtraHostAddresses() {
    return this.extraHostAddresses;
  }

  public void sendMessage(ByteBuffer bytes) throws IOException {
    final SctpAssociation association = this.clientAssociation;
    if (association == null) {
      throw new IOException("SCTP client is not connected");
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
    final SctpAssociation association = this.clientAssociation;
    return association != null && association.isConnected();
  }
}
