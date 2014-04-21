package com.spotify.helios.agent;

class ImagePullFailedException extends Exception {
  ImagePullFailedException(final Throwable cause) {
    super(cause);
  }

  ImagePullFailedException(final String message) {
    super(message);
  }
}