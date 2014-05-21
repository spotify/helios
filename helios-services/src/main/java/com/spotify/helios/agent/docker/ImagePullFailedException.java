package com.spotify.helios.agent.docker;

public class ImagePullFailedException extends DockerException {

  private final String image;

  public ImagePullFailedException(final String image, final Throwable cause) {
    super("Image pull failed: " + image, cause);
    this.image = image;
  }

  public ImagePullFailedException(final String image, final String message) {
    super("Image pull failed: " + image + ": " + message);
    this.image = image;
  }

  public String getImage() {
    return image;
  }
}