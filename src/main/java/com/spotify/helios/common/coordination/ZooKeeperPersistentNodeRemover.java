/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.common.coordination;

import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.DefaultReactor;
import com.spotify.helios.common.PersistentAtomicReference;
import com.spotify.helios.common.Reactor;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.spotify.helios.common.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.common.coordination.ZooKeeperOperations.delete;

public class ZooKeeperPersistentNodeRemover extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperPersistentNodeRemover.class);

  public static final TypeReference<List<String>> PATHS_TYPE =
      new TypeReference<List<String>>() {};
  private static final List<String> EMPTY_PATHS = Collections.emptyList();

  private static final long RETRY_INTERVAL = 5000;

  private final ZooKeeperClient client;
  private final Reactor reactor;

  private final PersistentAtomicReference<List<String>> front;
  private final PersistentAtomicReference<List<String>> back;
  private final Predicate<Node> predicate;

  private final Object lock = new Object() {};

  public ZooKeeperPersistentNodeRemover(final String name, final ZooKeeperClient client,
                                        final Path stateFile, final Predicate<Node> predicate)
      throws IOException {
    this.client = client;
    this.predicate = predicate;
    this.front = PersistentAtomicReference.create(stateFile.toString() + ".front", PATHS_TYPE,
                                                  Suppliers.ofInstance(EMPTY_PATHS));
    this.back = PersistentAtomicReference.create(stateFile.toString() + ".back", PATHS_TYPE,
                                                 Suppliers.ofInstance(EMPTY_PATHS));
    this.reactor = new DefaultReactor(name, new Update(), RETRY_INTERVAL);
  }

  public void remove(final String path) {
    while (true) {
      try {
        synchronized (lock) {
          final Set<String> mutable = Sets.newHashSet(front.get());
          mutable.add(path);
          front.set(ImmutableList.copyOf(mutable));
        }
        break;
      } catch (IOException e) {
        log.error("Error updating front", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          throw Throwables.propagate(ie);
        }
      }
    }
    reactor.update();
  }

  public static ZooKeeperPersistentNodeRemover create(final String name,
                                                      final ZooKeeperClient client,
                                                      final Path stateFile,
                                                      final Predicate<Node> predicate)
      throws IOException {
    return new ZooKeeperPersistentNodeRemover(name, client, stateFile, predicate);
  }

  @Override
  protected void startUp() throws Exception {
    reactor.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run() {
      // Drain the front to the backlog
      final Set<String> backPaths = Sets.newHashSet(back.get());
      synchronized (lock) {
        if (!front.get().isEmpty()) {
          final List<String> frontPaths = front.get();
          backPaths.addAll(frontPaths);
          try {
            back.set(ImmutableList.copyOf(backPaths));
            front.set(EMPTY_PATHS);
          } catch (IOException e) {
            log.error("Error draining front", e);
            throw Throwables.propagate(e);
          }
        }
      }

      // Remove all nodes in the backlog
      final Set<String> newBackPaths = Sets.newHashSet(backPaths);
      for (final String path : backPaths) {
        Node node = null;
        try {
          node = client.getNode(path);
        } catch (KeeperException.NoNodeException ignore) {
          // we're done here
          newBackPaths.remove(path);
        } catch (KeeperException.ConnectionLossException e) {
          log.warn("ZooKeeper connection lost while inspecting node: {}", path);
          throw Throwables.propagate(e);
        } catch (KeeperException e) {
          log.error("Failed inspecting node: {}", path);
        }
        if (node != null) {
          try {
            final boolean remove;
            try {
              remove = evaluate(node);
            } catch (Exception e) {
              log.error("Condition threw exception for node: {}", e, path);
              continue;
            }
            if (remove) {
              client.transaction(check(path, node.getStat().getVersion()),
                                 delete(path));
              // we're done here
              newBackPaths.remove(path);
              log.debug("Removed node: {}", path);
            }
          } catch (KeeperException.BadVersionException ignore) {
            // we're done here
            newBackPaths.remove(path);
          } catch (KeeperException.ConnectionLossException e) {
            log.warn("ZooKeeper connection lost while removing node: {}", path);
            throw Throwables.propagate(e);
          } catch (KeeperException e) {
            log.error("Failed removing node: {}", path);
          }
        }
      }

      try {
        final ImmutableList<String> newBackPathsList = ImmutableList.copyOf(newBackPaths);
        if (!back.get().equals(newBackPathsList)) {
          back.set(newBackPathsList);
        }
      } catch (IOException e) {
        log.error("Error writing back", e);
        throw Throwables.propagate(e);
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  private boolean evaluate(final Node node) {
    return predicate.apply(node);
  }
}
