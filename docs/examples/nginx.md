Create an nginx job using the nginx container image, exposing it on the host on port 8080

    helios create nginx:v1 nginx:1.7.1 -p http=80:8080

Check that the job is listed

    helios jobs

List helios hosts

    helios hosts

Deploy the nginx job on any of the hosts

    helios deploy nginx:v1 <host> ...

Check the job status

    helios status

Curl the nginx container when it's started running

    curl <host>:8080

Undeploy the nginx job

    helios undeploy -a nginx:v1

Remove the nginx job

    helios remove nginx:v1

