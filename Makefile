create-mysql-database:
	docker run -it --name mysql -e MYSQL_ROOT_PASSWORD='Gui@140294' -p 3306:3306 -d mysql:latest

run-mysql-service:
	mysql -h172.17.0.2 -uroot -pGui@140294