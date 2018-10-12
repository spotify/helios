#!/bin/bash -ex

case "$1" in
  pre_machine)
    # Edit pom files to have correct version syntax
    for i in $(find . -name pom.xml -not -path './.rvm*'); do sed -i 's/${revision}/0/g' $i; done

    ;;

  pre_machine_docker)
    # have docker bind to localhost
    docker_opts='DOCKER_OPTS="$DOCKER_OPTS -H tcp://0.0.0.0:2375"'
    sudo sh -c "echo '$docker_opts' >> /etc/default/docker"
    cat /etc/default/docker

    ;;

  post_machine)
    # fix permissions on docker.log so it can be collected as an artifact
    sudo chown ubuntu:ubuntu /var/log/upstart/docker.log

    ;;

  dependencies)
    mvn clean install -T 2 -Dmaven.javadoc.skip=true -DskipTests=true -B -V
    pip install codecov

    ;;

  verify_no_tests)
    mvn clean verify -DskipTests

    ;;

  test)
    # fix DOCKER_HOST to be accessible from within containers
    docker0_ip=$(/sbin/ifconfig docker0 | grep 'inet addr' | \
      awk -F: '{print $2}' | awk '{print $1}')
    export DOCKER_HOST="tcp://$docker0_ip:2375"

    case $CIRCLE_NODE_INDEX in
      0)
        # run all tests *except* helios-system-tests
        sed -i'' 's/<module>helios-system-tests<\/module>//' pom.xml
        mvn test -B -Pjacoco

        ;;

      1)
        # run DeploymentGroupTest in its own container since it takes forever
        mvn test -B -pl helios-system-tests -Dtest=com.spotify.helios.system.DeploymentGroupTest

        ;;

      2)
        # run helios-system-tests that start with A-G
        echo "%regex[com.spotify.helios.system.[H-Z].*]" >> .test-excludes
        echo "%regex[com.spotify.helios.system.DeploymentGroupTest.*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      3)
        # run helios-system-tests that start with H-R
        echo "%regex[com.spotify.helios.system.[A-GS-Z].*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      4)
        # run helios-system-tests that starts with S-Z
        echo "%regex[com.spotify.helios.system.[A-R].*]" >> .test-excludes
        mvn test -B -pl helios-system-tests

        ;;

      5)
        # build images for integration tests
        mvn -P build-images -P build-solo package -DskipTests=true -Dmaven.javadoc.skip=true \
          -B -V -pl helios-services

        # tag the helios-solo image we just built
        solo_image=$(cat helios-services/target/test-classes/solo-image.json | jq -r '.image')
        # docker tag -f deprecated in docker 1.10
        # https://github.com/marcuslonnberg/sbt-docker/issues/39
        docker tag $solo_image spotify/helios-solo:latest

        mvn verify -B -pl helios-integration-tests
        ;;

    esac

    ;;

  post_test)
    # collect artifacts into the artifacts dir
    find . -regex ".*/target/.*-[0-9]\.jar" | xargs -I {} mv {} $CIRCLE_ARTIFACTS
    find . -regex ".*/target/.*-SNAPSHOT\.jar" | xargs -I {} mv {} $CIRCLE_ARTIFACTS
    find . -regex ".*/target/.*\.deb" | xargs -I {} mv {} $CIRCLE_ARTIFACTS

    ;;

  collect_test_reports)
    cp */target/surefire-reports/*.xml $CIRCLE_TEST_REPORTS || true
    cp */target/failsafe-reports/*.xml $CIRCLE_TEST_REPORTS || true
    cp /tmp/helios-test/log/* $CIRCLE_TEST_REPORTS || true
    codecov

    ;;

esac
