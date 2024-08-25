## Setup
1. Copy `config_template.py` to `config_source.py` and `config_destination.py`
2. Edit `config.py` with your database(s) credentials. 
- The script is intended to take from a database `songs` and run dump the events into a new database `EventSim`
3. Run the main script

## Make a .evn
Create a .env file and fill it with the variables referenced in config source & config destination

be sure to do
export varible = 'nameofvar'
into your ternial to add it to you enviorment

## Create a .gitignore 
-add pycahce
     venv
     simulationdata 
     