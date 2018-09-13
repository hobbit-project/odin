#!/bin/bash



    git clone https://github.com/AKSW/TWIG.git
    # 2.
    cd TWIG
    mvn clean compile
    # 3. 
    mkdir data
    cd data
    # 4. 
    wget http://hobbitdata.informatik.uni-leipzig.de/TWIG/data.tar.gz
    tar -xzf data.tar.gz
    rm data.tar.gz
    # 5. 
    cd ../../

cd TWIG
#mkdir $5

export MAVEN_OPTS="-Xmx100G"

ARGS="Automaton data/word_matrix_0.obj data/message_count_0.obj data/time_count_0.obj $1 $2 $3 $4 $5"

mvn exec:java -Dexec.mainClass="org.aksw.twig.Main" -Dexec.args="$ARGS"
