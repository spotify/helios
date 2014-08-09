#!/bin/bash -ex

case "$1" in
  pre_machine)
    # have docker bind to localhost
    docker_opts='DOCKER_OPTS="$DOCKER_OPTS -H tcp://127.0.0.1:2375"'
    sudo sh -c "echo '$docker_opts' >> /etc/default/docker"

    cat /etc/default/docker

    ;;

  dependencies)
    mvn clean install -T 2 -Dmaven.javadoc.skip=true -DskipTests=true -B -V

    ;;

  pre_test)
    # clean the artifacts dir from the previous build
    rm -rf artifacts && mkdir artifacts

    ;;

  test)
    # expected parallelism: 2x. needs to be set in the project settings via CircleCI's UI.
    case $CIRCLE_NODE_INDEX in
      0)
        # run all tests *except* helios-system-tests
        sed -i'' 's/<module>helios-system-tests<\/module>//' pom.xml
        mvn test -B

        ;;

      1)
        # run helios-system-tests
        mvn test -B -pl helios-system-tests

        ;;

    esac

    ;;

  post_test)
    # collect artifacts into the artifacts dir
    find . -regex ".*/target/.*-[0-9]\.jar" | xargs -I {} mv {} artifacts
    find . -regex ".*/target/.*-SNAPSHOT\.jar" | xargs -I {} mv {} artifacts
    find . -regex ".*/target/.*\.deb" | xargs -I {} mv {} artifacts

    ;;

esac
