package com.svtlabs.jedis;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import redis.clients.jedis.JedisSocketFactory;
import redis.clients.jedis.Protocol;

class UdsJedisSocketFactory implements JedisSocketFactory {
  private final File udsSocket;

  UdsJedisSocketFactory(String pipePath) {
    udsSocket = new File(pipePath);
  }

  @Override
  public Socket createSocket() throws IOException {
    Socket socket = AFUNIXSocket.newStrictInstance();
    socket.connect(new AFUNIXSocketAddress(udsSocket), Protocol.DEFAULT_TIMEOUT);
    return socket;
  }

  @Override
  public String getDescription() {
    return udsSocket.toString();
  }

  @Override
  public String getHost() {
    return udsSocket.toString();
  }

  @Override
  public void setHost(String host) {}

  @Override
  public int getPort() {
    return 0;
  }

  @Override
  public void setPort(int port) {}

  @Override
  public int getConnectionTimeout() {
    return Protocol.DEFAULT_TIMEOUT;
  }

  @Override
  public void setConnectionTimeout(int connectionTimeout) {}

  @Override
  public int getSoTimeout() {
    return Protocol.DEFAULT_TIMEOUT;
  }

  @Override
  public void setSoTimeout(int soTimeout) {}
}
