#!/bin/bash

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

## cluster name from config file
CLUSTER_NAME=$(<${CURRENT_DIR}/../config/cluster_name.txt)

peg fetch ${CLUSTER_NAME}
# peg fetch ${CLUSTER_NAME}.db

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} flink
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka

###
## If you wish you can install redis on a single, separate node like so
# peg sshcmd-node ${CLUSTER_NAME}.db 1 "sudo apt-get install coreutils make gcc "\
# " && wget http://download.redis.io/redis-stable.tar.gz  -P ~/Downloads "\
# " && sudo tar zxvf ~/Downloads/redis-* -C /usr/local "\
# " && sudo mv /usr/local/redis-* /usr/local/redis "\
# " && echo export REDIS_HOME=/usr/local/redis >> ~/.profile "\
# " && export PATH=\${PATH}:\${REDIS_HOME}/src >> ~/.profile "\
# " && source ~/.profile "\
# " && cd \${REDIS_HOME} "\
# " && sudo make distclean && sudo make"

peg sshcmd-node ${CLUSTER_NAME} 1 "sudo pip install kafka-python"
