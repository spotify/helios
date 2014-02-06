package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.framework.api.transaction.CuratorTransaction;

public interface ZooKeeperOperation {
  void register(CuratorTransaction transaction) throws Exception;
}
