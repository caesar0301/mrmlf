#!/bin/bash
# Deploy maven artefact in current directory into Maven central repository
# using maven-release-plugin goals
logger="maven-central-deploy.log"

##
# If you set `autoReleaseAfterClose` false for nexus-staging-maven-plugin
# in pom.xml, you can manually inspect the staging repository in Nexus and
# trigger a release of the staging repository later on the Web or with
#
#    $ mvn nexus-staging:release
#
# If you find something went wrong you can drop the staging repository with:
#
#    $ mvn nexus-staging:drop
##
read -p "Really deploy to maven central repository  (yes/no)? "
if ( [ "$REPLY" == "yes" ] ) then
  mvn clean deploy  -e | tee $logger
else
  echo 'Exit without deploy'
fi