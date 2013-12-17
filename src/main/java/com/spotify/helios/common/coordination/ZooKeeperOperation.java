package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;

public interface ZooKeeperOperation {
  void register(CuratorTransaction transaction) throws Exception;
}
