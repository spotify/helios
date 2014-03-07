**Please join our mailing list:**
https://groups.google.com/a/spotify.com/forum/#!forum/helios

0.0.5
=====
Changes since the deployment in shared.cloud the Magneto team has used
the week of 2014-03-03 (specifically commit id dd8359b)

* RELATED: Many updates to the docker-maven-plugin -- please make sure
  you are using version 0.0.7 or later of the plugin.
* CLI: 'job history' command should now work again.
* CLI: Added optional matcher for 'host list' command so you can filter to
  get hosts which contain the substring
* CLI: Added 'host jobstatus' command to give the status of all jobs on the
  particular machine.  This is distinct from 'host jobs' which reports
  more of the intended state of things.
* Added PULLING_IMAGE state, to fix the percieved problem when you first
  deploy a job and it looks like nothing is happening, but it's just
  pulling the image down.
* Lots of alerting support.
* Make sure agent still functions as best it can if/when Docker acts up.
* Now allow the job id hash to be missing in the REST call to create jobs
  as creating the hash requires our Java code, which we shouldn't expect
  clients to have.
* Host now checks the version of the client (assuming it passes the
  X-Helios-Version header) so can warn if the client version should be
  updated, and can fail if it's sufficiently out of date.
* Improved testing.

