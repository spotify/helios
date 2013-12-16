package com.spotify.helios.common.coordination;

import com.netflix.curator.framework.api.transaction.CuratorTransaction;
import com.netflix.curator.framework.api.transaction.CuratorTransactionBridge;

public interface ZooKeeperOperation {
  CuratorTransactionBridge register(CuratorTransaction transaction) throws Exception;
}
