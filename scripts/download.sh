#!/bin/bash
#
# This script will execute the following points:
# 
# 1. Clones the repository 'https://github.com/AKSW/TWIG'
# 2. Executes 'build.sh' to build the application.
# 3. Downloads models to '/TWIG/sample/analysis'.
# 4. Runs the mimic by executing 'mimic.sh'
#
# Now in sample/output are the mimic data.
#
#################################################################
#
export MAVEN_OPTS="-Xmx100GB" 
# 1.
git clone https://github.com/AKSW/TWIG.git /TWIG
# 2.
cd /TWIG && ./build.sh
# 3. 
cd /TWIG/sample/analysis
# 4. 
wget http://hobbitdata.informatik.uni-leipzig.de/TWIG/data.tar.gz
tar -xzf data.tar.gz
rm data.tar.gz
echo "Deleted .tar"
# 5. 
cd ../../
echo "Now trying to run twig"

java -jar -Xmx100G TWIG/target/twig-parent-0.0.1-SNAPSHOT.jar Automaton TWIG/sample/analysis/word_matrix_0.obj TWIG/sample/analysis/message_count_0.obj TWIG/sample/analysis/time_count_0.obj $1 $2 $3 $4 $5
