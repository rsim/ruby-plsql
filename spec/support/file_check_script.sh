#!/bin/bash
set -e

FILE=/vagrant/oracle-xe-11.2.0-1.0.x86_64.rpm.zip

if [ ! -f "$FILE" ] ; then
  echo "Oracle XE database installation (oracle-xe-11.2.0-1.0.x86_64.rpm.zip) can not be found. Please download from Oracle homepage and put it into project home directory."
  exit 1
fi
