package org.jdiameter.client.impl.transport.sctp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sun.nio.sctp.SctpServerChannel;

import org.jdiameter.client.api.IMessage;
import org.jdiameter.client.api.io.IConnection;
import org.jdiameter.client.api.io.IConnectionListener;
import org.jdiameter.client.api.io.TransportException;
import org.jdiameter.client.api.parser.IMessageParser;
import org.jdiameter.server.impl.io.sctp.NetworkGuard;
import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * Exercises the SCTP client connection, server guard and server connection end to end over loopback.
 */
public class SctpTransportTest {

  private static final byte[] CLIENT_BYTES = new byte[] { 1, 2, 3 };
  private static final byte[] SERVER_BYTES = new byte[] { 4, 5, 6, 7 };

  private static class Recorder implements IConnectionListener {

    final CountDownLatch opened = new CountDownLatch(1);
    final CountDownLatch closed = new CountDownLatch(1);
    final BlockingQueue<byte[]> messages = new LinkedBlockingQueue<byte[]>();

    @Override
    public void connectionOpened(String connKey) {
      opened.countDown();
    }

    @Override
    public void connectionClosed(String connKey, List notSended) {
      closed.countDown();
    }

    @Override
    public void messageReceived(String connKey, IMessage message) {
      messages.add(((TaggedMessage) Proxy.getInvocationHandler(message)).payload);
    }

    @Override
    public void internalError(String connKey, IMessage message, TransportException cause) {
    }
  }

  private static class TaggedMessage implements InvocationHandler {

    final byte[] payload;

    TaggedMessage(byte[] payload) {
      this.payload = payload;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return null;
    }
  }

  private static IMessage message(byte[] payload) {
    return (IMessage) Proxy.newProxyInstance(IMessage.class.getClassLoader(), new Class<?>[] { IMessage.class }, new TaggedMessage(payload));
  }

  private static IMessageParser parser() {
    return (IMessageParser) Proxy.newProxyInstance(IMessageParser.class.getClassLoader(), new Class<?>[] { IMessageParser.class },
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) {
            if (method.getName().equals("encodeMessage")) {
              return ByteBuffer.wrap(((TaggedMessage) Proxy.getInvocationHandler(args[0])).payload);
            }
            if (method.getName().equals("createMessage")) {
              final ByteBuffer data = (ByteBuffer) args[0];
              final byte[] copy = new byte[data.remaining()];
              data.get(copy);
              return message(copy);
            }
            return null;
          }
        });
  }

  private static int freePort() throws Exception {
    final ServerSocket socket = new ServerSocket(0);
    try {
      return socket.getLocalPort();
    }
    finally {
      socket.close();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testClientServerExchangeAndGuardDestroy() throws Exception {
    try {
      SctpServerChannel.open().close();
    }
    catch (Exception e) {
      throw new SkipException("SCTP not available on this host: " + e);
    }

    final InetAddress loopback = InetAddress.getLoopbackAddress();
    final int port = freePort();

    final NetworkGuard guard = new NetworkGuard(loopback, port, parser());
    final BlockingQueue<IConnection> accepted = new LinkedBlockingQueue<IConnection>();
    guard.addListener(new org.jdiameter.server.api.io.INetworkConnectionListener() {
      @Override
      public void newNetworkConnection(IConnection connection) {
        accepted.add(connection);
      }
    });

    final SCTPClientConnection client = new SCTPClientConnection(parser());
    client.getClient().setOrigAddress(new java.net.InetSocketAddress(loopback, 0));
    client.getClient().setDestAddress(new java.net.InetSocketAddress(loopback, port));
    final Recorder clientEvents = new Recorder();
    client.addConnectionListener(clientEvents);
    client.connect();
    assertTrue(client.isConnected());

    final IConnection serverSide = accepted.poll(5, TimeUnit.SECONDS);
    assertNotNull(serverSide, "server must report the new connection");
    final Recorder serverEvents = new Recorder();
    serverSide.addConnectionListener(serverEvents);

    assertTrue(clientEvents.opened.await(5, TimeUnit.SECONDS));
    assertTrue(serverEvents.opened.await(5, TimeUnit.SECONDS));
    assertTrue(serverSide.isConnected());

    client.sendMessage(message(CLIENT_BYTES));
    assertEquals(serverEvents.messages.poll(5, TimeUnit.SECONDS), CLIENT_BYTES);

    serverSide.sendMessage(message(SERVER_BYTES));
    assertEquals(clientEvents.messages.poll(5, TimeUnit.SECONDS), SERVER_BYTES);

    client.disconnect();
    assertTrue(serverEvents.closed.await(5, TimeUnit.SECONDS), "server side must see the disconnect");

    client.release();
    serverSide.release();
    guard.destroy();

    // the listening socket must have been released
    NetworkGuard again = null;
    for (int i = 0; i < 50 && again == null; i++) {
      try {
        again = new NetworkGuard(loopback, port, parser());
      }
      catch (Exception e) {
        Thread.sleep(100);
      }
    }
    assertNotNull(again, "listening port must be released after destroy");
    again.destroy();
  }
}
