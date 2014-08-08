#!/bin/bash -e
git push origin master
TAGREF=refs/tags/$(git describe --abbrev=0 master)
git push origin ${TAGREF}:${TAGREF}
