#!/bin/bash

# Expects a list of Java file paths to be compiled and the target path as last argument.
# Each Java file path has to correspond to the package of this class.

# 1. Compiles each given Java file respecting Mahout dependencies using the `hadoop classpath` in the javac call and creates a Hadoop executable JAR containing both, all the Mahout code and the compiled Java files. 

# copies a file preserving the directory structure, e.g.
# 	copyWithDir a/b/c/file.txt target/
# would copy the file to target/a/b/c/file.txt and 
# create the necessary directories in target/
copyWithDir() { 
	mkdir -p -- "$(dirname -- "$2")" && cp -- "$1" "$2"
}

# the file must exist before this can be called
getAbsPath() {
	# $1 : relative filename
	echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

MAHOUT_HOME=~/mahout_unpacked
TMP=$MAHOUT_HOME/tmp

echo „Compiling given Java files with Mahout and Hadoop class path“

# get the last parameter which is the output path
for targetRel; do true; done
target=$(getAbsPath $targetRel)

# prepare a fresh copy of the original Mahout job JAR
mkdir $TMP
cp $MAHOUT_HOME/mahout-mrlegacy-1.0-SNAPSHOT-job.jar_backup $target

startWorkingDir=$pwd

for file; do
	# last parameter is the target path, not a Java file to be processed
	if [ "$file" == "$targetRel" ]; then break; fi

	echo "processing $file"

	fileCopy=$TMP/$file
	copyWithDir $file $fileCopy

	cd $MAHOUT_HOME
	javac -classpath `hadoop classpath`:. $fileCopy
	
	# merge the compiled Java file into the target JAR
	echo "merging in $fileCopy"
	cd $TMP	
	jar uf $target ${file%.java}.class

	cd $startWorkingDir
done

rm -r $TMP
cd $startWorkingDir