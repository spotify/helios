If you have multiple Helios masters, you can setup DNS records so the CLI can discover masters automatically for your domain:

    helios -s example.net

What we're actually doing for the `-s` flag is looking up the `_spotify-helios._http.services.<site>` SRV record, and selecting a master from the returned endpoints.
