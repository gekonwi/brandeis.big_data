#!/bin/bash

# =============================================================================================================
# Expects a list of Java file paths to be compiled and the target path as last argument.
# Each Java file path has to correspond to the package of this class.
# E.g. the path to hadoop08.read_clusters.ClusterDumper must be hadoop08/read_clusters/ClusterDumper.java.
# Thus there must be a hadoop08/read_clusters/ClusterDumper.java in the directory from which this script is called.
# =============================================================================================================


# 1. Compiles each given Java file respecting Mahout dependencies using the `hadoop classpath` in the javac call and creates a Hadoop executable JAR containing both, all the Mahout code and the compiled Java files. 


# the file must exist before this can be called
getAbsPath() {
	# $1 : relative filename
	echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

MAHOUT_HOME=~/mahout_examples_unpacked

echo „Compiling given Java files with Mahout and Hadoop class path“

# get the last parameter which is the output path
for targetRel; do true; done
target=$(getAbsPath $targetRel)

# prepare a fresh copy of the original Mahout job JAR
cp $MAHOUT_HOME/mahout-examples-1.0-SNAPSHOT-job.jar_backup $target

startWorkingDir=$PWD


javaFilesRel=""
javaFilesAbs=""
for file; do
	# last parameter is the target path, not a Java file to be processed
	if [ "$file" == "$targetRel" ]; then break; fi

	javaFilesRel="$javaFilesRel $file"
	javaFilesAbs="$javaFilesAbs $(getAbsPath $file)"
done


echo "compiling $javaFilesRel"
cd $MAHOUT_HOME
javac -classpath `hadoop classpath`:. $javaFilesAbs
cd $startWorkingDir


javaFilesRelArray=( $javaFilesRel )
for file in "${javaFilesRelArray[@]}"; do
	
	# merge the compiled Java file into the target JAR
	compiledFile=${file%.java}.class
	echo "merging in $compiledFile"
	jar uf $target $compiledFile
done