If you have multiple Helios masters, you can setup DNS records so the CLI can discover masters
automatically for your domain:

    helios -d example.net

What we're actually doing for the `-d` flag is looking up the `_helios._https.<domain>` SRV record,
falling back to `_helios._http.<domain>` if no HTTPS SRV records are found,
and selecting a master from the returned endpoints.
