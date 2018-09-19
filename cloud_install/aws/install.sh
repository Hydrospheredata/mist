#!/bin/bash

apt-get update
apt-get install -y nginx openjdk-8-jdk apache2-utils

JAVA=$(which java)

source ~/.aws_setup_data

ssh-keygen -b 2048 -t rsa -f ~/.mist_key -q -N ""
chmod 600 ~/.mist_key

echo "<p>Mist will start soon</p>" > /var/www/html/index.html
cat << EOF > /etc/nginx/sites-enabled/default
server {
	listen 80 default_server;
	listen [::]:80 default_server;

	root /var/www/html;
	index index.html index.htm index.nginx-debian.html;

	server_name _;
}
EOF
service nginx restart

cd /opt

wget http://repo.hydrosphere.io/hydrosphere/static/preview/mist-1.0.0-RC17.tar.gz
tar xvfz mist-1.0.0-RC17.tar.gz
mv mist-1.0.0-RC17 mist

wget https://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz 
tar xvfz spark-2.3.0-bin-hadoop2.7.tgz
mv spark-2.3.0-bin-hadoop2.7 spark

# By some reasons, there is some problems with running Mist right after intance was started
# InfoProvider can't connect to master
sleep 30

INSTANCE_ID=$(ec2metadata --instance-id)
$JAVA -cp /opt/mist/utils/aws-init-setup.jar io.hydrosphere.mist.aws.Main $INSTANCE_ID $ACCESS_KEY_ID $ACCESS_KEY_SECRET $AWS_REGION /opt/mist/configs/default.conf ~/.mist_key.pub

SPARK_HOME=/opt/spark /opt/mist/bin/mist-master start

htpasswd -b -c /etc/nginx/.htpasswd $MIST_LOGIN $MIST_PASSWORD
cat << EOF > /etc/nginx/sites-enabled/default
server {
	listen 80 default_server;
	listen [::]:80 default_server;

	root /var/www/html;
	index index.html index.htm index.nginx-debian.html;

	server_name _;

	location / {
	   proxy_pass http://127.0.0.1:2004/;
     auth_basic "Restricted";
     auth_basic_user_file /etc/nginx/.htpasswd;
	}

	location /v2/api/ws {
	    proxy_pass         http://127.0.0.1:2004/v2/api/ws;
	    proxy_http_version 1.1;
      proxy_set_header Upgrade \$http_upgrade;
      proxy_set_header Connection "upgrade";
      auth_basic "Restricted";
      auth_basic_user_file /etc/nginx/.htpasswd;
	}

}
EOF
service nginx restart
