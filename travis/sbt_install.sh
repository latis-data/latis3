#!/usr/bin/env bash

SBT_EXTRAS_HASH=6db3d3d1c38082dd4c49cce9933738d9bff50065

curl -sL https://raw.githubusercontent.com/paulp/sbt-extras/$SBT_EXTRAS_HASH/sbt > /tmp/sbt
chmod +x /tmp/sbt
sudo mv /tmp/sbt /usr/local/bin/sbt
