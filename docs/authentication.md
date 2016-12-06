# Authentication with Helios

When using HTTPS, Helios client can present an X.509 client certificate to verify
the user's identity. By presenting a digital signature to the Helios master, the
client proves that it is who it claims to be.

Note that only the client supports HTTPS and client certificates. The master does not. The cluster administrator must add the missing pieces that validate certificates and act accordingly. Suggestions for how such a setup might look can be found below.

Generally this type of mutual TLS authentication requires the user to have their
own SSL certificate issued by some certificate authority (CA). Often, this
will be your company or organization's CA. However, for users who do not have a
CA or digital certificates but do have OpenSSH keys, the Helios client can
generate certificates on-the-fly based on the user's SSH key.

## Mutual TLS authentication

To enable mutual TLS authentication, you must use HTTPS endpoints for the Helios
master. To do this:

1. Place the Helios master behind nginx or another web application server.

2. Ensure that your server is requesting client certificates. For example, with
   nginx, set the [`ssl_verify_client` option](http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_verify_client)
   to any value other than `off`.

3. Use the options for your web server to validate the certificates in your
   preferred way. For example, at Spotify, we have nginx pass the SSL certificate
   to a microservice that ensures that the UID and public key match a known user
   in our LDAP database.

## Client certificates

The Helios client will now need to present a client certificate to authenticate.
This can be either a pre-issued X.509 certificate file and associated private
key, or a certificate generated based on your SSH key.

First we'll explain briefly how the ssh-agent signed certificates work, and then go through how to supply credentials for the `helios` CLI and the Helios API respectively.

### On-the-fly certificates with ssh-agent

If you have an OpenSSH key and use [ssh-agent](http://linux.die.net/man/1/ssh-agent)
(you probably do if you use `ssh`), Helios can generate an X.509 certificate for
you on the fly. Here's how it works:

1. The Helios client checks for a running SSH agent, and if one is found, asks
   for a list of your public SSH keys. Private keys are never exposed by
   ssh-agent.

2. The Helios client generates a temporary X.509 certificate containing your username.
   It then signs it with your private SSH key via ssh-agent and sends it to the HTTPS server.

3. The server should then look up the public key for the username specified in the certificate and
   use it to verify that the certificate was signed by the corresponding private key. If the
   signature is successfully verified we know that the user is who they claim to be.
   **Note that this step must be implemented by the cluster administrator.**

When using the Helios CLI the username in the generated certificate comes from the `--username` argument, which defaults to your login user. When using the API the username has to be set explicitly via the `HeliosClient.Builder.setUser()` method.

The actual handshake procedure is carried out by the TLS implementation -
the only thing the Helios client does it generate and sign the X.509 certificate used, instead
of using an existing certificate.

**It is important to note that the generated certificate is signed only by your
SSH key**, and verifying its signature only proves that you have the private key corresponding
to your SSH public key. The web server must still decide whether or not to accept
your certificate. At Spotify, we've configured nginx to pass the certificate to
another tiny service which extracts the public key from the certificate and
verifies that it matches the public SSH key of one of our employees.

### Authenticating with the `helios` CLI

When using the CLI there are two ways to supply a certificate to the server when needed:

 * Set the `HELIOS_CERT_PATH` environment variable to a path containing the pre-issued X.509 certificate and corresponding private key, named `cert.pem` and `key.pem` respectively, or
 * Let the Helios client generate a temporary X.509 certificate that is signed with your private SSH key via ssh-agent, as described above.

The latter is typically not suitable for non-interactive use cases (e.g. a script run periodically by cron). If the `HELIOS_CERT_PATH` environment variable is set this method takes precedence.

### Authenticating with the Helios API

When you use the Helios API (via the `HeliosClient` class) you have the same options as when using the CLI, as explained above. You can also explicitly set the path where the certificate and key is via the `HeliosClient.Builder.setCertKeyPaths()` method. If a certificate path is set explicitly the `HELIOS_CERT_PATH` is ignored.

Again, for non-interactive use cases relying on ssh-agent is typically not a suitable solution so we recommend that you specify a path either explicitly or via the `HELIOS_CERT_PATH` environment variable.
