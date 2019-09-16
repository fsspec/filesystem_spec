#!/usr/bin/env bash
# https://docs.travis-ci.com/user/docker/#using-docker-compose 


DOCKER_COMPOSE_VERSION=${DOCKER_COMPOSE_VERSION:-1.23.2}

# Install docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get -y -o Dpkg::Options::="--force-confnew" install docker-ce

# Update docker-compose
sudo rm /usr/local/bin/docker-compose
curl -L https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-`uname -s`-`uname -m` > docker-compose
chmod +x docker-compose
sudo mv docker-compose /usr/local/bin

# install FUSE
sudo apt-get install libfuse-dev

# install conda
source $(dirname $BASH_SOURCE)/install_conda.sh
