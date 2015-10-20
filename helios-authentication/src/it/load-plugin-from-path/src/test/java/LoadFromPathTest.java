import com.spotify.helios.auth.AuthenticationPlugin;
import com.spotify.helios.auth.AuthenticationPluginLoader;
import com.spotify.helios.auth.ServerAuthenticationConfig;

import org.junit.Test;

import java.io.File;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;


public class LoadFromPathTest {

  public static final String PLUGIN_JAR_PATH = "pluginJarPath";

  @Test
  public void test() {
    // ensure that the plugin being loaded is not actually on the classpath of this JVM
    try {
      final String className = "com.spotify.helios.auth.it.IntegrationTestPlugin";
      Class.forName(className);
      fail("Plugin to be loaded from path (" + className + ") is actually on classpath");
    } catch (ClassNotFoundException e) {
      // cool, continue
    }

    final String path = System.getProperty(PLUGIN_JAR_PATH);
    assertNotNull("Cannot test loading from path if system property '" + PLUGIN_JAR_PATH
                  + "' is not supplied", path);

    System.out.printf("Trying to load from path %s%n", path);

    final ServerAuthenticationConfig config = new ServerAuthenticationConfig();
    config.setEnabledScheme("plugin-for-integration-test");
    config.setPluginPath(new File(path).toPath());

    final AuthenticationPlugin<?> plugin = AuthenticationPluginLoader.load(config);

    assertThat(plugin.schemeName(), is(config.getEnabledScheme()));
  }
}