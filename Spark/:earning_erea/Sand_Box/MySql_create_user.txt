sudo -i
mysql -u root -p

CREATE USER 'niv2'@'localhost' IDENTIFIED WITH caching_sha2_password BY '7124175';
GRANT ALL PRIVILEGES ON *.* TO 'niv2'@'localhost';
GRANT SELECT, INSERT, UPDATE ON mydatabase.* TO 'niv2'@'localhost';
FLUSH PRIVILEGES;
EXIT;
GRANT ALL PRIVILEGES ON *.* TO 'niv2'@'localhost' IDENTIFIED BY '7124175' WITH GRANT OPTION;



SELECT host FROM mysql.user WHERE user = 'niv2';


  git config --global user.email "nivgoldberg1@gmail.com"
  git config --global user.name "niv"



ALTER USER 'root'@'localhost' IDENTIFIED BY '7124175';