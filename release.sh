#!/bin/bash -e

if [ `git symbolic-ref HEAD 2>/dev/null` != "refs/heads/master" ]
then
	echo "This is not the master branch."
	exit 1
fi

RELEASE_VERSION=$1
if [[ -n "$RELEASE_VERSION" ]]
then
  RELEASE_ARGS="-DreleaseVersion=$RELEASE_VERSION"
fi

# Use the maven release plugin to update the pom versions and tag the release commit
mvn -B release:prepare release:clean $RELEASE_ARGS

echo "Created release tag" `git describe --abbrev=0 master`
echo "Remember to: ./push-release.sh"
