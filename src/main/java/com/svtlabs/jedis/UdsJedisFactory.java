package com.svtlabs.jedis;

import redis.clients.jedis.Jedis;

class UdsJedisFactory {
  private final UdsJedisSocketFactory socketFactory;

  public UdsJedisFactory(String pipePath) {
    socketFactory = new UdsJedisSocketFactory(pipePath);
  }

  public Jedis create() {
    return new Jedis(socketFactory);
  }
}
