#!/bin/bash -ex

case "$1" in
  pre_machine)
    # ensure correct level of parallelism
    expected_nodes=5
    if [ "$CIRCLE_NODE_TOTAL" -ne "$expected_nodes" ]
    then
        echo "Parallelism is set to ${CIRCLE_NODE_TOTAL}x, but we need ${expected_nodes}x."
        exit 1
    fi

    # have docker bind to localhost
    docker_opts='DOCKER_OPTS="$DOCKER_OPTS -H tcp://0.0.0.0:2375"'
    sudo sh -c "echo '$docker_opts' >> /etc/default/docker"

    cat /etc/default/docker

    # Edit pom files to have correct version syntax
    for i in $(find . -name pom.xml -not -path './.rvm*'); do sed -i 's/${revision}/0/g' $i; done

    ;;

  post_machine)
    # fix permissions on docker.log so it can be collected as an artifact
    sudo chown ubuntu:ubuntu /var/log/upstart/docker.log

    ;;

  dependencies)
    mvn clean install -T 2 -Dmaven.javadoc.skip=true -DskipTests=true -B -V

    ;;

  pre_test)
    # clean the artifacts dir from the previous build
    rm -rf artifacts && mkdir artifacts

    ;;

  test)
    case $CIRCLE_NODE_INDEX in
      0)
        # run all tests *except* helios-system-tests
        sed -i'' 's/<module>helios-system-tests<\/module>//' pom.xml
        mvn test -B

        ;;

      1)
        # run helios-system-tests that start with A-G
        echo "%regex[com.spotify.helios.system.[H-Z].*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      2)
        # run helios-system-tests that start with H-R
        echo "%regex[com.spotify.helios.system.[A-GS-Z].*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      3)
        # run helios-system-tests that starts with S-Z
        echo "%regex[com.spotify.helios.system.[A-R].*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      4)
        sudo apt-get install -y jq

        # build images for integration tests
        mvn -P build-images -P build-solo package -DskipTests=true -Dmaven.javadoc.skip=true \
          -B -V -pl helios-services

        # tag the helios-solo image we just built
        solo_image=$(cat helios-services/target/test-classes/solo-image.json | jq -r '.image')
        docker tag -f $solo_image spotify/helios-solo:latest

        # fix DOCKER_HOST to be accessible from within containers
        docker0_ip=$(/sbin/ifconfig docker0 | grep 'inet addr' | \
          awk -F: '{print $2}' | awk '{print $1}')
        export DOCKER_HOST="tcp://$docker0_ip:2375"

        # bring up helios-solo for integration tests
        cd solo
        # need to patch helios-up to not docker run --rm, since --rm doesn't work on CircleCI
        sed -i 's/docker run --rm/docker run/' ./helios-up
        ./helios-up
        cd ..

        mvn verify -B -pl helios-integration-tests
        ;;

    esac

    ;;

  post_test)
    # collect artifacts into the artifacts dir
    find . -regex ".*/target/.*-[0-9]\.jar" | xargs -I {} mv {} artifacts
    find . -regex ".*/target/.*-SNAPSHOT\.jar" | xargs -I {} mv {} artifacts
    find . -regex ".*/target/.*\.deb" | xargs -I {} mv {} artifacts

    ;;

  collect_test_reports)
    cp */target/surefire-reports/*.xml $CI_REPORTS || true
    cp */target/failsafe-reports/*.xml $CI_REPORTS || true

    ;;

esac
