#!/bin/bash -ex

case "$1" in
  pre_machine)
    # ensure correct level of parallelism
    expected_nodes=6
    if [ "$CIRCLE_NODE_TOTAL" -ne "$expected_nodes" ]
    then
        echo "Parallelism is set to ${CIRCLE_NODE_TOTAL}x, but we need ${expected_nodes}x."
        exit 1
    fi

    # edit pom files to have correct version syntax
    for i in $(find . -name pom.xml -not -path './.rvm*'); do sed -i 's/${revision}/0/g' $i; done

    # install docker CLI
    DOCKER_VERSION=17.03.1-ce
    curl -L -o /tmp/docker-${DOCKER_VERSION}.tgz \
      https://get.docker.com/builds/Linux/x86_64/docker-${DOCKER_VERSION}.tgz
    tar -xz -C /tmp -f /tmp/docker-${DOCKER_VERSION}.tgz
    mv /tmp/docker/* /usr/bin

    # install other build dependencies
    apt-get update && apt-get install -y python-pip lsof
    pip install codecov

    ;;

  build)
    mvn clean install -T 2 -Dmaven.javadoc.skip=true -DskipTests=true -B -V

    ;;

  test)
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
        apt-get install -y jq

        # build images for integration tests
        mvn -P build-images -P build-solo package -DskipTests=true -Dmaven.javadoc.skip=true \
          -B -V -pl helios-services

        # tag the helios-solo image we just built
        solo_image=$(cat helios-services/target/test-classes/solo-image.json | jq -r '.image')
        docker tag -f $solo_image spotify/helios-solo:latest

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
