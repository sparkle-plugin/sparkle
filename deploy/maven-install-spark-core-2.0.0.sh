## maven install spark core to local repository
echo "mvn install: $1" 

if [ ! -f $1 ]; then
  echo "jar file $1 does not exist"
  exit
fi

GROUP_ID=org.apache.spark
ARTIFACT_ID=spark-local-hpc
VERSION_ID=2.0.0
PACKAGING=jar
GENERATEPOM=true

mvn install:install-file -Dfile=$1 -DgroupId=$GROUP_ID -DartifactId=$ARTIFACT_ID -Dversion=$VERSION_ID -Dpackaging=$PACKAGING -DgeneratePom=$GENERATEPOM
