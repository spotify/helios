package com.spotify.helios.agent.docker;

public class ImageNotFoundException extends DockerException {

  private final String image;

  public ImageNotFoundException(final String image, final Throwable cause) {
    super("Image not found: " + image, cause);
    this.image = image;
  }

  public ImageNotFoundException(final String image, final String message) {
    super("Image not found: " + image + ": " + message);
    this.image = image;
  }

  public ImageNotFoundException(final String image) {
    super("Image not found: " + image);
    this.image = image;
  }

  public String getImage() {
    return image;
  }
}