#git

sudo apt-get install git

#mongo 

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo service mongod start

# depedencias para JAVA
#maven2
sudo apt-get install maven2

# depedencias para pyhton
#pip
sudo apt-get install python-setuptools python-dev build-essential
sudo easy_install pip 
sudo pip install --upgrade virtualenv 
#twitter 
pip install twitter
pip install TwitterSearch
#boto api aws
pip install boto
#pyhton conector mongo
pip install pymongo
#Redis 
sudo pip install redis

