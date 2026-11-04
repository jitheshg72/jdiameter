package org.jdiameter.common.impl.transport.sctp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

import org.testng.SkipException;
import org.testng.annotations.Test;

public class SctpAssociationTest {

  private static class Recorder implements SctpAssociation.Listener {

    final CountDownLatch up = new CountDownLatch(1);
    final CountDownLatch down = new CountDownLatch(1);
    final BlockingQueue<byte[]> payloads = new LinkedBlockingQueue<byte[]>();
    volatile int ppid = -1;

    @Override
    public void onCommunicationUp() {
      up.countDown();
    }

    @Override
    public void onCommunicationShutdown() {
      down.countDown();
    }

    @Override
    public void onPayload(byte[] data, int streamNumber, int payloadProtocolId) {
      ppid = payloadProtocolId;
      payloads.add(data);
    }
  }

  private static SctpServerChannel openServer(InetAddress address) {
    try {
      final SctpServerChannel server = SctpServerChannel.open();
      server.bind(new InetSocketAddress(address, 0));
      return server;
    }
    catch (Exception e) {
      throw new SkipException("SCTP not available on this host: " + e);
    }
  }

  @Test
  public void testRoundTripFragmentationAndShutdown() throws Exception {
    final InetAddress loopback = InetAddress.getLoopbackAddress();
    final SctpServerChannel server = openServer(loopback);

    try {
      final InetSocketAddress serverAddress = (InetSocketAddress) server.getAllLocalAddresses().iterator().next();

      final SctpChannel clientChannel = SctpAssociation.connect(new InetSocketAddress(loopback, 0), null, serverAddress, 5000);
      final SctpChannel serverSide = server.accept();

      final Recorder clientEvents = new Recorder();
      final Recorder serverEvents = new Recorder();
      final SctpAssociation client = new SctpAssociation(clientChannel, "client", 65535, clientEvents);
      // tiny read buffer on purpose: messages must be reassembled
      final SctpAssociation accepted = new SctpAssociation(serverSide, "server", 1024, serverEvents);
      client.start();
      accepted.start();

      assertTrue(clientEvents.up.await(1, TimeUnit.SECONDS));
      assertTrue(serverEvents.up.await(1, TimeUnit.SECONDS));
      assertTrue(client.isConnected());

      client.send(new byte[] { 1, 2, 3 }, 0, 46);
      assertEquals(serverEvents.payloads.poll(5, TimeUnit.SECONDS), new byte[] { 1, 2, 3 });
      assertEquals(serverEvents.ppid, 46);

      accepted.send(new byte[] { 9, 8 }, 0, 46);
      assertEquals(clientEvents.payloads.poll(5, TimeUnit.SECONDS), new byte[] { 9, 8 });

      final byte[] big = new byte[10000];
      for (int i = 0; i < big.length; i++) {
        big[i] = (byte) i;
      }
      client.send(big, 0, 46);
      assertEquals(serverEvents.payloads.poll(5, TimeUnit.SECONDS), big);

      client.close();
      assertTrue(serverEvents.down.await(5, TimeUnit.SECONDS), "peer must notice the shutdown");
      assertTrue(clientEvents.down.await(5, TimeUnit.SECONDS), "local close must notify too");
      assertFalse(client.isConnected());
      assertFalse(accepted.isConnected());
    }
    finally {
      server.close();
    }
  }

  @Test
  public void testConnectToClosedPortFails() throws Exception {
    final InetAddress loopback = InetAddress.getLoopbackAddress();
    openServer(loopback).close();
    try {
      SctpAssociation.connect(new InetSocketAddress(loopback, 0), null, new InetSocketAddress(loopback, 1), 2000);
      fail("connect must fail when nobody listens");
    }
    catch (IOException expected) {
      // expected
    }
  }
}
