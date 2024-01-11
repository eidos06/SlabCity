This folder contains database generation scripts for each problem in LeetCode and Calcite.

To generate databases, follow these steps:

* (Optional) Create a virtual environment and activate it
```bash
python3 -m venv venv
source venv/bin/activate
```

* Install all required dependencies
```bash
pip install -r requirements.txt
```

* Execute the generation script
```bash
python3 generate_all.py
```

After executing these commands, you should see a folder named `databases` under this folder. Each subfolder in `databases` contains a database for each problem. The database is stored as CSV files. Extra scripts may be needed to import the csv files into your database system.
