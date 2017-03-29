/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.client;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

public class HeliosClientTest {

  private final RequestDispatcher dispatcher = mock(RequestDispatcher.class);
  private final HeliosClient client = new HeliosClient("test", dispatcher);

  @Test(expected = IllegalStateException.class)
  public void testBuildWithNoEndpoints() {
    HeliosClient.newBuilder()
        .setEndpoints(Collections.<URI>emptyList())
        .build();
  }

  @Test
  public void listHosts() throws Exception {
    final List<String> hosts = ImmutableList.of("foo1", "foo2", "foo3");

    mockResponse("GET", hasPath("/hosts/"), response("GET", 200, hosts));

    assertThat(client.listHosts().get(), equalTo(hosts));
  }

  private static Response response(final String method, int statusCode, Object payload) {
    // second param is request URI, which is not relevant here
    return new Response(method, null, statusCode, Json.asBytesUnchecked(payload),
        Collections.<String, List<String>>emptyMap());
  }

  @SuppressWarnings("unchecked")
  private void mockResponse(final String method,
                            final Matcher<URI> uriMatcher,
                            final Response response) {

    final byte[] emptyRequestPayload = new byte[0];
    when(dispatcher.request(argThat(uriMatcher), eq(method), eq(emptyRequestPayload), anyMap()))
        .thenReturn(Futures.immediateFuture(response));
  }

  /** A Matcher that tests that the URI has a path equal to the given path. */
  private static Matcher<URI> hasPath(final String path) {
    return new FeatureMatcher<URI, String>(Matchers.equalTo(path), "path", "path") {
      @Override
      protected String featureValueOf(final URI actual) {
        return actual.getPath();
      }
    };
  }

  /**
   * A Matcher that tests that the rawQuery of the URI (i.e. encoded) contains the given substring.
   */
  private static Matcher<URI> containsQuery(final String rawQuerySubstring) {
    return new FeatureMatcher<URI, String>(Matchers.containsString(rawQuerySubstring), "query",
        "query") {
      @Override
      protected String featureValueOf(final URI actual) {
        return actual.getRawQuery();
      }
    };
  }

  @Test
  public void listHostsFilterByNamePattern() throws Exception {
    final List<String> hosts = ImmutableList.of("foo1", "foo2", "foo3");

    mockResponse("GET",
        allOf(hasPath("/hosts/"), containsQuery("namePattern=foo")),
        response("GET", 200, hosts));

    assertThat(client.listHosts("foo").get(), equalTo(hosts));
  }

  @Test
  public void listHostsFilterBySelectors() throws Exception {
    final List<String> hosts = ImmutableList.of("foo1", "foo2", "foo3");

    mockResponse("GET",
        allOf(hasPath("/hosts/"),
            containsQuery("selector=foo%3Dbar"),
            containsQuery("selector=site%3Dabc")
        ),
        response("GET", 200, hosts));

    final Set<String> selectors = ImmutableSet.of("foo=bar", "site=abc");
    assertThat(client.listHosts(selectors).get(), equalTo(hosts));
  }

  private static Map<JobId, Job> fakeJobs(JobId... jobIds) {
    Map<JobId, Job> jobs = new HashMap<>();
    for (JobId jobId : jobIds) {
      final Job job = Job.newBuilder()
          .setName(jobId.getName())
          .setVersion(jobId.getVersion())
          .setHash(jobId.getHash())
          .build();
      jobs.put(jobId, job);
    }
    return jobs;
  }

  @Test
  public void listJobs() throws Exception {
    final Map<JobId, Job> jobs = fakeJobs(JobId.parse("foobar:v1"));

    mockResponse("GET",
        hasPath("/jobs"),
        response("GET", 200, jobs)
    );

    assertThat(client.jobs().get(), is(jobs));
  }

  @Test
  public void listJobsWithJobFilter() throws Exception {
    final Map<JobId, Job> jobs = fakeJobs(JobId.parse("foobar:v1"));

    mockResponse("GET",
        allOf(
            hasPath("/jobs"),
            containsQuery("q=foo")
        ),
        response("GET", 200, jobs)
    );

    assertThat(client.jobs("foo").get(), is(jobs));
  }

  @Test
  public void listJobsWithJobAndHostFilter() throws Exception {
    final Map<JobId, Job> jobs = fakeJobs(JobId.parse("foobar:v1"));

    mockResponse("GET",
        allOf(
            hasPath("/jobs"),
            containsQuery("q=foo"),
            containsQuery("hostPattern=bar")
        ),
        response("GET", 200, jobs)
    );

    assertThat(client.jobs("foo", "bar").get(), is(jobs));
  }
}
