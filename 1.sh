export DB="glc20241002"
export HOST="127.0.0.1"
mysql -u mguuji -h $HOST -p -e "drop database $DB ;"  
rm *.qmd
python3 create_glc.py --db $DB --schema schema.sql  
mysql -h $HOST -u mguuji -p $DB -e "LOAD DATA LOCAL INFILE 'useragents.txt' INTO TABLE user_agents (agent)"  
#python3 ./urledit.py --db $DB --action add_from_json --json_file lists.json 
python3 ./urljson.py --db $DB --action import --json_file lists.json
python3 ./glc.py --db $DB --force --no-toot
python3 ./lldebug.py --db $DB
find . -name "*.qmd" -mmin -1 | xargs -I {} quarto render {}
python3 ./glc.py --db $DB --force --no-toot
python3 ./lldebug.py --db $DB
find . -name "*.qmd" -mmin -1 | xargs -I {} quarto render {}
