#!/usr/bin/env python
import sys
import os
import os.path
import xml.dom.minidom

if os.environ["TRAVIS_SECURE_ENV_VARS"] == "false":
  print "no secure env vars available, skipping deployment"
  sys.exit()

homedir = os.path.expanduser("~")

m2 = xml.dom.minidom.parse(homedir + '/.m2/settings.xml')
settings = m2.getElementsByTagName("settings")[0]

serversNodes = settings.getElementsByTagName("servers")
if not serversNodes:
  serversNode = m2.createElement("servers")
  settings.appendChild(serversNode)
else:
  serversNode = serversNodes[0]

profilesNodes = settings.getElementsByTagName("profiles")
if not profilesNodes:
  profilesNode = m2.createElement("profiles")
  settings.appendChild(profilesNodes)
else:
  profilesNode = profilesNodes[0]

activeProfilesNodes = settings.getElementsByTagName("activeProfiles")
if not activeProfilesNodes:
  activeProfilesNode = m2.createElement("activeProfiles")
  settings.appendChild(activeProfilesNode)
else:
  activeProfilesNode = activeProfilesNodes[0]

def make_server_node(idText, serversNode):
  sonatypeServerNode = m2.createElement("server")
  sonatypeServerId = m2.createElement("id")
  sonatypeServerUser = m2.createElement("username")
  sonatypeServerPass = m2.createElement("password")
  
  idNode = m2.createTextNode(idText)
  userNode = m2.createTextNode(os.environ["SONATYPE_USERNAME"])
  passNode = m2.createTextNode(os.environ["SONATYPE_PASSWORD"])
  
  sonatypeServerId.appendChild(idNode)
  sonatypeServerUser.appendChild(userNode)
  sonatypeServerPass.appendChild(passNode)
  
  sonatypeServerNode.appendChild(sonatypeServerId)
  sonatypeServerNode.appendChild(sonatypeServerUser)
  sonatypeServerNode.appendChild(sonatypeServerPass)
  
  serversNode.appendChild(sonatypeServerNode)

def make_profile_node(profilesNode):
  profileNode = m2.createElement("profile")
  profileIdNode = m2.createElement("id")
  profileId = m2.createTextNode("ossrh")
  profileIdNode.appendChild(profileId)
  profileNode.appendChild(profileIdNode)

  activationNode = m2.createElement("activation")
  byDefaultNode = m2.createElement("activeByDefault")
  trueNode = m2.createTextNode("true")
  byDefaultNode.appendChild(trueNode)
  activationNode.appendChild(byDefaultNode)
  profileNode.appendChild(activationNode)

  propertiesNode = m2.createElement("properties")
  keynameNode = m2.createElement("gpg.keyname")
  keyname = m2.createTextNode("Helios Project")
  keynameNode.appendChild(keyname)
  propertiesNode.appendChild(keynameNode)
  passphraseNode = m2.createElement("gpg.passphrase")
  passphrase = m2.createTextNode(os.environ["SONATYPE_GPG_PASSPHRASE"])
  passphraseNode.appendChild(passphrase)
  propertiesNode.appendChild(passphraseNode)

  profileNode.appendChild(propertiesNode)
  profilesNode.appendChild(profileNode)

def make_active_profile_node(activeProfilesNodes):
  activeProfileNode = m2.createElement("activeProfile")
  profileName = m2.createTextNode("ossrh")
  activeProfileNode.appendChild(profileName)
  activeProfilesNodes.appendChild(activeProfileNode)

make_server_node("sonatype-nexus-snapshots", serversNode)
make_server_node("sonatype-nexus-staging", serversNode)
make_profile_node(profilesNode)
make_active_profile_node(activeProfilesNode)
m2Str = m2.toprettyxml()
f = open(homedir + '/.m2/mySettings.xml', 'w')
f.write(m2Str)
f.close()
