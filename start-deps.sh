#!/bin/sh
docker kill dump-db
docker kill dump-db-gui

docker volume create dump-db

docker run --rm v dump-db:/var/lib/mysql -d --name dump-db -p 3306:3306 \
  -e MYSQL_DATABASE=foo \
  -e MYSQL_USER=foo \
  -e MYSQL_PASSWORD=bar \
  -e MYSQL_RANDOM_ROOT_PASSWORD=1 \
  mariadb:10

docker run --rm --name dump-db-gui -p 8080:80 --link dump-db:db -d phpmyadmin/phpmyadmin