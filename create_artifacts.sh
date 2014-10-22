#!/bin/bash -e

# Tar up the staged release, all the pom files, and some build files. We will use these in
# subsequent build steps to perform the actual release.
tar -zcvf target/helios-staged-release.tar.gz `find . -name nexus-staging && find . -name pom.xml`

# Copy all debian packages into target/debs, and tar them into target/helios-debs.tar.gz
mkdir target/debs
find . -name *.deb -type f -not -path "./target/*" -exec cp {} target/debs/ \;
tar -C target/debs -zcf target/helios-debs.tar.gz .

# Output build version into file for later Jenkins items
VERSION=$(egrep -o '<version>.*</version>' -m 1 pom.xml | sed 's/<version>\(.*\)<\/version>/\1/')
echo ${VERSION} > target/version
