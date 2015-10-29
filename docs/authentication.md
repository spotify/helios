# Authentication with Helios

When using HTTPS, Helios client can present an X.509 client certificate to verify
the user's identity. By presenting a digital signature to the Helios master, the
client proves that it is who it claims to be.

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

### Existing X.509 certificate

This is actually not yet implemented.

### On-the-fly certificates with ssh-agent

If you have an OpenSSH key and use [ssh-agent](http://linux.die.net/man/1/ssh-agent)
(you probably do if you use `ssh`), Helios can generate an X509 certificate for
you on the fly. Here's how it works:

1. The Helios client checks for a running SSH agent, and if one is found, asks
   for a list of your public SSH keys. Private keys are never exposed by
   ssh-agent.

2. The Helios client generates a self-signed X.509 certificate containing your
   SSH public key and username, which it sends to the HTTPS server.

3. To prove that you own the corresponding private key, the Helios client asks
   ssh-agent to cryptographically hash and sign part of the TLS handshake with
   the server.

The hashing and signing in the final step is a standard part of the TLS handshake
and how the client proves to the server that it has possession of the private key.
The only difference in our implementation is that we ask ssh-agent to do the
signing, since Helios doesn't have access to your private key.

**It is important to note that the generated certificate is a self-signed
certificate**, and all it proves is that you have the private key corresponding
to your SSH public key. The web server must still decide whether or not to accept
your certificate. At Spotify, we've configured nginx to pass the certificate to
another tiny service which extracts the public key from the certificate and
verifies that it matches the public SSH key of one of our employees.
