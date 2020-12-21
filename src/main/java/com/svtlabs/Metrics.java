package com.svtlabs;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class Metrics {
  private final Map<String, Long> stats = new ConcurrentHashMap<>();
  private final Set<String> counters = new HashSet<>();

  public Metrics() {}

  public void measure(String name, Runnable action) {
    long totalTime = stats.getOrDefault(name, 0L);
    long start = System.nanoTime();
    try {
      action.run();
    } finally {
      totalTime += System.nanoTime() - start;
      stats.put(name, totalTime);
    }
  }

  public <T> T measure(String name, Supplier<T> action) {
    long totalTime = stats.getOrDefault(name, 0L);
    long start = System.nanoTime();
    try {
      return action.get();
    } finally {
      totalTime += System.nanoTime() - start;
      stats.put(name, totalTime);
    }
  }

  public Context start(String name) {
    return new Context(name);
  }

  public void incrBy(String name, long delta) {
    long cur = stats.getOrDefault(name, 0L);
    stats.put(name, cur + delta);
    counters.add(name);
  }

  public void clear() {
    stats.clear();
  }

  @Override
  public String toString() {
    // Walk through keys in alpha order; timings are in nanosecs, so convert to microsecs.
    // Counters should be emitted as-is.
    StringBuilder sb = new StringBuilder();
    TreeSet<String> statsKeys = new TreeSet<>(stats.keySet());
    statsKeys.forEach(
        (name) ->
            sb.append("  ")
                .append(name)
                .append(": ")
                .append(counters.contains(name) ? stats.get(name) : stats.get(name) / 1000));
    return sb.toString();
  }

  public class Context {
    private final String name;
    private final long start;

    public Context(String name) {
      this.name = name;
      start = System.nanoTime();
    }

    public void stop() {
      long totalTime = stats.getOrDefault(name, 0L);
      totalTime += System.nanoTime() - start;
      stats.put(name, totalTime);
    }
  }
}
