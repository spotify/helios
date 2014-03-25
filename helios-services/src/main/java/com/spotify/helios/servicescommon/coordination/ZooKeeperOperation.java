package com.spotify.helios.servicescommon.coordination;

import org.apache.zookeeper.Op;

import java.util.Collection;

public interface ZooKeeperOperation {
  void register(Collection<Op> operations) throws Exception;
}
