# Helios

If you're looking for how to use Helios, see the
[Wiki](https://ghe.spotify.net/helios/helios/wiki), and most probably
the [User
Manual](https://ghe.spotify.net/helios/helios/wiki/Helios-User-Manual)
is what you're looking for.

# Building and Testing Helios

You'll want Docker installed somewhere.  If you have Vagrant
installed, it should be a simple matter of checking out the 
[helios-vagrant](https://ghe.spotify.net/helios/helios-vagrant)
repo, and following the instructions there to bring it up.  If you want to run it 
hosted on your actual machine, contact the NYCSI squad first, but you'll need our
custom fork of Docker, an installation of ZooKeeper > 3.4.0 and probably some other
handholding.

There's also
[boot2docker](https://github.com/boot2docker/boot2docker) which can work
to some degree, but you may run into problems that we've fixed
by patching Docker, but the unit tests **will** pass with boot2docker.
If you insist, you can also install docker yourself on your machine,
but Vagrant is really the best choice as we've patched Docker in a few
ways that are important, as well as that there are subsidiary
executables and configuration bits that are already dealt with in the
Vagrant image, that you'd have to replicate, and IMHO, it's just not
worth the hassle.  Additionally with Vagrant, if things go sideways, you 
can just nuke the image and start over.

Actually building Helios and running it's tests should be a simple matter
of running:

    mvn package

The launcher scripts are in `bin/`

# The Nickel Tour

The source for the Helios Master and Agent is under `helios-services`.
The CLI source is under `helios-tools`.  The Helios Java client is
under `helios-client`.  The main meat of the Helios Agent is in
`Supervisor.java` which revolves around the lifecycle of managing
individual running Docker containers.  For the master, the http
response handlers are in
`src/main/java/com/spotify/helios/master/resources`.  The interactions
with ZooKeeper are mainly in `ZooKeeperMasterModel.java` and
`ZooKeeperAgentModel.java`.

The Helios services use [Dropwizard](http://dropwizard.io) which is a
bundle of Jetty, Jersey, Jackson, Yammer Metrics, Guava, Logback and
other Java libraries.

# Releasing

    # Run tests and create a tagged release commit
    ./release.sh

    # Push it
    git push origin master
    git push origin release
    git push origin --tags
