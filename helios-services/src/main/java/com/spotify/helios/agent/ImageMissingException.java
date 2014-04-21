package com.spotify.helios.agent;

class ImageMissingException extends Exception {

  ImageMissingException(final String message) {
    super(message);
  }
}