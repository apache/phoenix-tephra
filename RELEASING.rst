=======================
Releasing Apache Tephra
=======================

This page describes the step-by-step process of how to perform an official Apache Tephra version release,
including deploying the release artifacts to Maven repositories and the additional administrative
steps to complete the release process.

Prerequisites
=============

Maven Settings File
-------------------

Prior to performing an Apache Tephra release, you must have an entry such as this in your
``~/.m2/settings.xml`` file to authenticate when deploying the release artifacts::

  <?xml version="1.0" encoding="UTF-8"?>
  <settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd"
      xmlns="http://maven.apache.org/SETTINGS/1.0.0"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <servers>
      <server>
        <id>apache.snapshots.https</id>
        <username>USERNAME</username>
        <password>PASSWORD</password>
      </server>
      <server>
        <id>apache.releases.https</id>
        <username>USERNAME</username>
        <password>PASSWORD</password>
      </server>
    </servers>
  </settings>
  
Replace ``USERNAME`` and ``PASSWORD`` with the correct values for your user account.  See the
`Maven Encryption Guide <http://maven.apache.org/guides/mini/guide-encryption.html>`_ for details
on how to avoid storing the plaintext password in the settings.xml file.

PGP Key
-------

You will also need to have created a PGP (or GPG) key pair, which will be used in signing the release
artifacts.  For more information on using the Maven GPG plugin, see this `introduction
<http://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/>`_ from Sonatype and
the Maven GPG Plugin `usage page
<https://maven.apache.org/plugins/maven-gpg-plugin/usage.html>`_.  You may also want to run
gpg-agent in order to avoid being prompted multiple times for the GPG key passphrase when
performing a release.


Performing the Release
======================

Ensure Local Branch is Up-to-date
---------------------------------

First, make sure your local copy of the ``master`` branch is up-to-date with all changes::

  git checkout master
  git pull

Create the Release Branch
-------------------------

Next, create a release branch from ``master``::

  git checkout -b release/N.N.N

replacing ``N.N.N`` with the desired release version.

Prepare the Release
-------------------

While on the release branch, prepare the release::
  
  mvn clean release:prepare -P apache-release
  
This will prompt you for the release version and the git tag to use for the release.  By
convention, we use ``vN.N.N`` for the release tag (ie. v0.6.0 for release 0.6.0).

Perform the Release
-------------------

Perform the release by running::
  
  mvn release:perform -P apache-release

This will checkout the source code using the release tag, build the release and deploy it to the
repository.apache.org repository. Also it creates a source tarball
``apache-tephra-0.8.0-incubating-SNAPSHOT-source-release.tar.gz`` in ``target`` directory.

Prepare Release Artifacts
-------------------------

1. Checkin the source release tarball, together with the signature, md5 and sha512 files found in
   ``target/`` directory
   to ``dist.apache.org/repos/dist/dev/incubator/tephra/${RELEASE_VERSION}-incubating-rc1/src/``.
2. Create a CHANGES.txt file to describe the changes in the release and checkin the file to
   ``dist.apache.org/repos/dist/dev/incubator/tephra/${RELEASE_VERSION}-incubating-rc1/CHANGES.txt``.
3. Close the staging repository at https://repository.apache.org


Update POM Version in master
----------------------------

Update the POMs in ``master`` by::

  git checkout master
  git merge release/N.N.N
  git push origin master

Vote for the Release in Dev Mailing List
----------------------------------------

Create a vote in the dev@tephra mailing list, and wait for 72 hours for the vote result.
Here is a template for the email::

  Subject: [VOTE] Release of Apache Tephra-${RELEASE_VERSION}-incubating [rc1]
  ============================================================================

  Hi all,

  This is a call for a vote on releasing Apache Tephra ${RELEASE_VERSION}-incubating, release candidate 1. This
  is the [Nth] release of Tephra.

  The source tarball, including signatures, digests, etc. can be found at:
  https://dist.apache.org/repos/dist/dev/incubator/tephra/${RELEASE_VERSION}-incubating-rc1/src

  The tag to be voted upon is v${RELEASE_VERSION}-incubating:
  https://git-wip-us.apache.org/repos/asf?p=incubator-tephra.git;a=shortlog;h=refs/tags/v${RELEASE_VERSION}-incubating

  The release hash is [REF]:
  https://git-wip-us.apache.org/repos/asf?p=incubator-tephra.git;a=commit;h=[REF]

  The Nexus Staging URL:
  https://repository.apache.org/content/repositories/orgapachetephra-[STAGE_ID]

  Release artifacts are signed with the following key:
  [URL_TO_SIGNER_PUBLIC_KEY]

  KEYS file available:
  https://dist.apache.org/repos/dist/dev/incubator/tephra/KEYS

  For information about the contents of this release, see:
  https://dist.apache.org/repos/dist/dev/incubator/tephra/${RELEASE_VERSION}-incubating-rc1/CHANGES.txt

  Please vote on releasing this package as Apache Tephra ${RELEASE_VERSION}-incubating

  The vote will be open for 72 hours.

  [ ] +1 Release this package as Apache Tephra ${RELEASE_VERSION}-incubating
  [ ] +0 no opinion
  [ ] -1 Do not release this package because ...

  Thanks,
  [YOUR_NAME]

Consolidate Vote Result
-----------------------

After the vote is up for 72 hours and having at least three +1 binding votes and no -1 votes,
close the vote by replying to the voting thread. Here is a template for the reply email::

  Subject: [RESULT][VOTE] Release of Apache Tephra-${RELEASE_VERSION}-incubating [rc1]
  ==================================================================================

  Hi all,

  After being opened for over 72 hours, the vote for releasing Apache Tephra
  ${RELEASE_VERSION}-incubating passed with n binding +1s and no 0 or -1.

  Binding +1s:
  [BINDING_+1_NAMES]

  I am going to create a vote in the general@ list.

  Thanks,
  [YOUR_NAME]

Vote for the Release from IPMC
------------------------------

1. Create a vote in the general@ mailing list for the IPMC to vote for the release.
2. Wait for 72 hours for the vote result. Use the same template as the dev vote, with the addition
   of links to the dev vote and result mail thread.
3. After the vote in general@ is completed with at least three +1 binding votes, close the vote by
   replying to the voting thread.

Release the Staging Repository in Artifactory
---------------------------------------------

Release the artifact bundle in Artifactory:

1. Login to https://repository.apache.org.
2. Go to "Staging Repos".
3. Find the "orgapachetephra" repo with the Tephra release.  Be sure to expand the contents of the
   repo to confirm that it contains the correct Tephra artifacts. 
4. Click on the "Release" button at top, and enter a brief description, such as "Apache Tephra N.N.N
   release".

Announcing and Completing the Release
=====================================

Mark the release as complete in JIRA (in the Apache Tephra Administration section):

1. Add a release for the next version, if necessary
2. Set a release date and release the released version

Release the source tarball:

1. Copy the release artifacts and CHANGES.txt from the dev to release directory at
   ``dist.apache.org/repos/dist/release/incubator/tephra/${RELEASE_VERSION}-incubating``

Finally, announce the release on the mailing lists: dev@tephra and announce@
