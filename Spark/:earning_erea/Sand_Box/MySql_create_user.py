sudo -i
mysql -u root -p

CREATE USER 'niv2'@'localhost' IDENTIFIED WITH caching_sha2_password BY '7124175';
GRANT ALL PRIVILEGES ON *.* TO 'niv2'@'localhost';
GRANT SELECT, INSERT, UPDATE ON mydatabase.* TO 'niv2'@'localhost';
FLUSH PRIVILEGES;
EXIT;
