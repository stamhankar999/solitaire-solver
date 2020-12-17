package com.svtlabs.jedis;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.BitSet;

class EncodingUtil {
  private EncodingUtil() {}

  static String encode(byte[] boardState) {
    return Base64.getEncoder().encodeToString(boardState);
  }

  static String encode(ByteBuffer boardState) {
    return encode(boardState.array());
  }

  static String encode(BitSet boardState) {
    return encode(boardState.toByteArray());
  }

  static byte[] decodeToBytes(String boardStateString) {
    return Base64.getDecoder().decode(boardStateString);
  }

  static ByteBuffer decodeToByteBuffer(String boardStateString) {
    return ByteBuffer.wrap(decodeToBytes(boardStateString));
  }
}
