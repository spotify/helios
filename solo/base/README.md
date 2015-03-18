helios-solo-base
===

This image contains dependencies for spotify/helios-solo. The only reason it exists is to speed up
building the spotify/helios-solo image.

Build & release
---
This image is built and released manually. If you make changes, bump `version.txt` and then run
`make push` to make and push the image. The reason we do this is to avoid unnecessarily rebuilding
this image with every Helios build when it rarely changes.

Doing this also speeds up `helios-use latest` for clients, since most of the layers for helios-solo
never change.
