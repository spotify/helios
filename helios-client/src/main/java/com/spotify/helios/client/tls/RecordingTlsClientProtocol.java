/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.client.tls;

import org.bouncycastle.crypto.tls.ByteQueue;
import org.bouncycastle.crypto.tls.ContentType;
import org.bouncycastle.crypto.tls.HandshakeType;
import org.bouncycastle.crypto.tls.TlsClientProtocol;
import org.bouncycastle.crypto.tls.TlsUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

/**
 * A {@link org.bouncycastle.crypto.tls.TlsClientProtocol} that keeps a complete record of all
 * handshake messages (for later hashing and signing by ssh-agent).
 */
class RecordingTlsClientProtocol extends TlsClientProtocol {

  private ByteArrayOutputStream record = new ByteArrayOutputStream();
  private ByteQueue handshakeQueue = new ByteQueue();

  private boolean recording = true;

  public RecordingTlsClientProtocol(InputStream input, OutputStream output,
                                    SecureRandom secureRandom) {
    super(input, output, secureRandom);
  }

  public byte[] getRecord() {
    return record.toByteArray();
  }

  @Override
  protected void sendClientKeyExchangeMessage() throws IOException {
    super.sendClientKeyExchangeMessage();
    recording = false;
  }

  @Override
  protected void safeWriteRecord(short type, byte[] buf, int offset, int len) throws IOException {
    if (recording && (type == ContentType.handshake)) {
      record.write(buf, offset, len);
    }

    super.safeWriteRecord(type, buf, offset, len);
  }

  @Override
  protected void processRecord(short protocol, byte[] buf, int offset, int len) throws IOException {
    if (recording && (protocol == ContentType.handshake)) {
      handshakeQueue.addData(buf, offset, len);
      processHandshakeQueue();
    }

    super.processRecord(protocol, buf, offset, len);
  }

  private void processHandshakeQueue() {
    boolean read;
    do {
      read = false;

      // we need at least 4 bytes to tell what the message type is, otherwise we
      // can't make the decision on whether it goes into the record
      if (handshakeQueue.available() >= 4) {
        // read the header that tells us the message type and content length
        byte[] beginning = new byte[4];
        handshakeQueue.read(beginning, 0, 4, 0);
        short type = TlsUtils.readUint8(beginning, 0);
        int len = TlsUtils.readUint24(beginning, 1);

        // based on the content length we just read, only proceed if we've received
        // all the content for the message
        if (handshakeQueue.available() >= (len + 4)) {
          byte[] buffer = handshakeQueue.removeData(len, 4);

          switch (type) {
            case HandshakeType.hello_request:
              break;
            case HandshakeType.finished:
              break;
            default:
              // this is a type of data that we do indeed want to record
              record.write(beginning, 0, 4);
              record.write(buffer, 0, len);
              break;
          }

          read = true; // we read some data, so there might be some more. loop again.
        }
      }
    } while (read);
  }

}
