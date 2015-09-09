package com.spotify.helios.common;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

import com.spotify.dns.DnsSrvResolver;
import com.spotify.dns.LookupResult;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ResolverTest {

  @Mock
  DnsSrvResolver resolver;

  @Test
  public void testSupplier() throws Exception {
    final List<LookupResult> lookupResults = ImmutableList.of(
        LookupResult.create("master1.example.com", 443, 1, 1, 1),
        LookupResult.create("master2.example.com", 443, 1, 1, 1),
        LookupResult.create("master3.example.com", 443, 1, 1, 1)
    );
    final URI[] expectedUris = {
        new URI("https://master1.example.com:443"),
        new URI("https://master2.example.com:443"),
        new URI("https://master3.example.com:443")
    };

    when(resolver.resolve("_helios._https.example.com")).thenReturn(lookupResults);

    final Supplier<List<URI>> supplier = Resolver.supplier("helios", "example.com", resolver);
    final List<URI> uris = supplier.get();

    assertThat(uris.size(), equalTo(3));
    assertThat(uris, Matchers.containsInAnyOrder(expectedUris));
  }

  @Test
  public void testSupplierWithHttpFallback() throws Exception {
    final List<LookupResult> lookupResults = ImmutableList.of(
        LookupResult.create("master1.example.com", 80, 1, 1, 1),
        LookupResult.create("master2.example.com", 80, 1, 1, 1),
        LookupResult.create("master3.example.com", 80, 1, 1, 1)
    );
    final URI[] expectedUris = {
        new URI("http://master1.example.com:80"),
        new URI("http://master2.example.com:80"),
        new URI("http://master3.example.com:80")
    };

    when(resolver.resolve("_helios._https.example.com"))
        .thenReturn(Collections.<LookupResult>emptyList());
    when(resolver.resolve("_helios._http.example.com")).thenReturn(lookupResults);

    final Supplier<List<URI>> supplier = Resolver.supplier("helios", "example.com", resolver);
    final List<URI> uris = supplier.get();

    assertThat(uris.size(), equalTo(3));
    assertThat(uris, Matchers.containsInAnyOrder(expectedUris));
  }
}
