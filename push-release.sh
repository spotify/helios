#!/bin/bash -e
git push origin master
git push origin release
TAGREF=refs/tags/$(git describe --abbrev=0 release)
git push origin ${TAGREF}:${TAGREF}
