package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class ContainerState {

  @JsonProperty("Running") private boolean running;
  @JsonProperty("Pid") private int pid;
  @JsonProperty("ExitCode") private int exitCode;
  @JsonProperty("StartedAt") private Date startedAt;
  @JsonProperty("FinishedAt") private Date finishedAt;

  public boolean running() {
    return running;
  }

  public void running(final boolean running) {
    this.running = running;
  }

  public int pid() {
    return pid;
  }

  public void pid(final int pid) {
    this.pid = pid;
  }

  public int exitCode() {
    return exitCode;
  }

  public void exitCode(final int exitCode) {
    this.exitCode = exitCode;
  }

  public Date startedAt() {
    return startedAt;
  }

  public void startedAt(final Date startedAt) {
    this.startedAt = startedAt;
  }

  public Date finishedAt() {
    return finishedAt;
  }

  public void finishedAt(final Date finishedAt) {
    this.finishedAt = finishedAt;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerState that = (ContainerState) o;

    if (exitCode != that.exitCode) {
      return false;
    }
    if (pid != that.pid) {
      return false;
    }
    if (running != that.running) {
      return false;
    }
    if (finishedAt != null ? !finishedAt.equals(that.finishedAt) : that.finishedAt != null) {
      return false;
    }
    if (startedAt != null ? !startedAt.equals(that.startedAt) : that.startedAt != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = (running ? 1 : 0);
    result = 31 * result + pid;
    result = 31 * result + exitCode;
    result = 31 * result + (startedAt != null ? startedAt.hashCode() : 0);
    result = 31 * result + (finishedAt != null ? finishedAt.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("running", running)
        .add("pid", pid)
        .add("exitCode", exitCode)
        .add("startedAt", startedAt)
        .add("finishedAt", finishedAt)
        .toString();
  }
}
