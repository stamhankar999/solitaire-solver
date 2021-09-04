package com.svtlabs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class BoardRelationManager {
  private final String baseDir;

  public BoardRelationManager(String baseDir) {
    this.baseDir = baseDir;
  }

  public static void main(String[] args) {
    CacheLoader<String, String> loader =
        new CacheLoader<String, String>() {
          @Override
          public String load(String key) {
            return key.toUpperCase();
          }
        };

    RemovalListener<String, String> listener;
    listener =
        new RemovalListener<String, String>() {
          @Override
          public void onRemoval(RemovalNotification<String, String> n) {
            if (n.wasEvicted()) {
              System.out.println("Evicting with value " + n.getValue());
            }
          }
        };

    LoadingCache<String, String> cache =
        CacheBuilder.newBuilder().maximumSize(2).removalListener(listener).build(loader);
    cache.getUnchecked("first");
    cache.getUnchecked("second");
    cache.getUnchecked("first");
    cache.getUnchecked("third");
    System.out.println(cache.size());
    System.out.println(cache.getIfPresent("first"));
    System.out.println(cache.getIfPresent("second"));
    System.out.println(cache.getIfPresent("third"));

    // NOTES on board relation processing:
    // 1st byte of child is sub1
    // 2nd byte of child is sub2
    // bytes 3-4 are filename. (so upto 64k files in dir)
    // rest is data in file
    // Cache key is ByteBuffer or byte[]; value if OutputStream
  }
}
