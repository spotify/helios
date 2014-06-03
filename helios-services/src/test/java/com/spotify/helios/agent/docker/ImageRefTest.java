package com.spotify.helios.agent.docker;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class ImageRefTest {

  @Test
  public void testImageWithoutTag() {
    final ImageRef sut = new ImageRef("foobar");
    assertThat(sut.getImage(), equalTo("foobar"));
    assertThat(sut.getTag(), is(nullValue()));
  }

  @Test
  public void testImageWithTag() {
    final ImageRef sut = new ImageRef("foobar:12345");
    assertThat(sut.getImage(), equalTo("foobar"));
    assertThat(sut.getTag(), is("12345"));
  }

  @Test
  public void testImageWithTagAndRegistry() {
    final ImageRef sut = new ImageRef("registry:4711/foo/bar:12345");
    assertThat(sut.getImage(), equalTo("registry:4711/foo/bar"));
    assertThat(sut.getTag(), is("12345"));
  }

}