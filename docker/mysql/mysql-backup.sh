mysqldump -hlocalhost -P3306 -uroot -ppwd159357 --all-databases --lock-all-tables | gzip > /mysql-backup/full.$(date +%y%m%d.%H%M%S).sql.gz
