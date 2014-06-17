/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon.coordination;

import com.google.common.base.Predicate;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.servicescommon.DefaultReactor;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.Reactor;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.reverse;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.check;
import static com.spotify.helios.servicescommon.coordination.ZooKeeperOperations.delete;

public class ZooKeeperPersistentNodeRemover extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperPersistentNodeRemover.class);

  private static final boolean DEFAULT_RECURSIVE = false;

  private static final long RETRY_INTERVAL_MILLIS = 5000;

  public static final TypeReference<List<String>> PATHS_TYPE =
      new TypeReference<List<String>>() {};
  private static final List<String> EMPTY_PATHS = Collections.emptyList();


  private final ZooKeeperClientProvider provider;
  private final Reactor reactor;

  private final PersistentAtomicReference<List<String>> front;
  private final PersistentAtomicReference<List<String>> back;
  private final Predicate<Node> predicate;

  private final boolean recursive;

  private final Object lock = new Object() {};

  public ZooKeeperPersistentNodeRemover(final String name, final ZooKeeperClientProvider provider,
                                        final Path stateFile, final Predicate<Node> predicate)
      throws IOException, InterruptedException {
    this(name, provider, stateFile, predicate, DEFAULT_RECURSIVE);
  }

  public ZooKeeperPersistentNodeRemover(final String name, final ZooKeeperClientProvider provider,
                                        final Path stateFile, final Predicate<Node> predicate,
                                        final boolean recursive)
      throws IOException, InterruptedException {
    this.provider = provider;
    this.predicate = predicate;
    this.front = PersistentAtomicReference.create(stateFile.toString() + ".front", PATHS_TYPE,
                                                  Suppliers.ofInstance(EMPTY_PATHS));
    this.back = PersistentAtomicReference.create(stateFile.toString() + ".back", PATHS_TYPE,
                                                 Suppliers.ofInstance(EMPTY_PATHS));
    this.reactor = new DefaultReactor(name, new Update(), RETRY_INTERVAL_MILLIS);
    this.recursive = recursive;
  }

  public void remove(final String path) throws InterruptedException {
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
        Thread.sleep(1000);
      }
    }
    reactor.signal();
  }

  public static ZooKeeperPersistentNodeRemover create(final String name,
                                                      final ZooKeeperClientProvider provider,
                                                      final Path stateFile,
                                                      final Predicate<Node> predicate)
      throws IOException, InterruptedException {
    return new ZooKeeperPersistentNodeRemover(name, provider, stateFile, predicate);
  }

  public static ZooKeeperPersistentNodeRemover create(final String name,
                                                      final ZooKeeperClientProvider provider,
                                                      final Path stateFile,
                                                      final Predicate<Node> predicate,
                                                      final boolean recursive)
      throws IOException, InterruptedException {
    return new ZooKeeperPersistentNodeRemover(name, provider, stateFile, predicate, recursive);
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
    public void run(final boolean timeout) throws InterruptedException {
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
      final ZooKeeperClient client = provider.get("persistent_remover");
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
              final List<String> nodes = Lists.newArrayList();
              if (recursive) {
                nodes.addAll(reverse(client.listRecursive(path)));
              } else {
                nodes.add(path);
              }
              client.transaction(check(path, node.getStat().getVersion()),
                                 delete(nodes));
              // we're done here
              newBackPaths.remove(path);
              log.debug("Removed node: {}", path);
            }
          } catch (KeeperException.BadVersionException | KeeperException.NoNodeException ignore) {
            // we're done here
            newBackPaths.remove(path);
          } catch (KeeperException.ConnectionLossException e) {
            log.warn("ZooKeeper connection lost while removing node: {}", path);
            throw Throwables.propagate(e);
          } catch (KeeperException e) {
            log.error("Failed removing node: {}", path, e);
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
