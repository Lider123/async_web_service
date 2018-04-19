import os
import pandas as pd
import requests

URL = "http://localhost:5000"
DATA_DIR = "data"

field = "Age"
count = 10
in_file = "titanic.csv"

in_path = os.path.join(DATA_DIR, in_file)
df = pd.read_csv(in_path).to_json(orient="split")
data = {
    "data": df,
    "field": field,
    "count": count
}
# r = requests.post(URL + "/top", data=data)
# r = requests.get(URL + "/top", data={"field": "Name", "count": 5})
r = requests.post(URL + "/upload", data=data)
print(r.text)
