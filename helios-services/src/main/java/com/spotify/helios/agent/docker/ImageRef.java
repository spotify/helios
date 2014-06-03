package com.spotify.helios.agent.docker;

public class ImageRef {

  private final String image;
  private final String tag;

  public ImageRef(final String image) {
    final int lastColon = image.lastIndexOf(':');
    if (lastColon < 0) {
      this.image = image;
      this.tag = null;
    } else {
      final String tag = image.substring(lastColon + 1);
      if (tag.indexOf('/') < 0) {
        this.image = image.substring(0, lastColon);
        this.tag = tag;
      } else {
        this.image = image;
        this.tag = null;
      }
    }
  }

  public String getImage() {
    return image;
  }

  public String getTag() {
    return tag;
  }

  @Override
  public String toString() {
    return tag == null ? image : image + ':' + tag;
  }
}
