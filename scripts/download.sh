#!/bin/bash
#
# This script will execute the following points:
# 
# 1. Clones the repository 'https://github.com/AKSW/TWIG'
# 2. Executes 'build.sh' to build the application.
# 3. Downloads models to '/TWIG/sample/analysis'.
# 4.  Runs the mimic by executing 'mimic.sh'
#
# Now in sample/output are the mimic data.
#
#################################################################
#
# 1.
git clone https://github.com/AKSW/TWIG.git
# 2.
cd TWIG
./build.sh
# 3. 
cd sample/analysis
# 4. 
wget http://hobbitdata.informatik.uni-leipzig.de/TWIG/data.tar.gz
tar -xzf data.tar.gz
rm data.tar.gz
# 5. 
cd ../../

dir="target/"
jar_name="twig-parent-0.0.1-SNAPSHOT.jar"
auto="Automaton"
objects_path="sample/analysis/"
matrix="word_matrix_0.obj"
message="message_count_0.obj"
time="time_count_0.obj"

java -jar target/twig-parent-0.0.1-SNAPSHOT.jar Automaton sample/analysis/word_matrix_0.obj sample/analysis/message_count_0.obj sample/analysis/time_count_0.obj $1 $2 $3 $4 $5
