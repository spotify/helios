#!/bin/bash -e

if [ `git symbolic-ref HEAD 2>/dev/null` != "refs/heads/master" ]
then
	echo "This is not the master branch."
	exit 1
fi

# Use the maven release plugin to update the pom versions and tag the release commit
mvn -B release:prepare release:clean

# Fast-forward release branch to the tagged release commit
git checkout release
git merge --ff-only `git describe --abbrev=0 master`
git checkout master

echo "Created release tag" `git describe --abbrev=0 master`
echo "Remember to: ./push-release.sh"
