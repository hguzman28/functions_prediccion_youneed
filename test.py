import pandas as pd
from datetime import datetime


dt_prediccion = pd.read_csv("./base_prediccion2.csv")

print(dt_prediccion.tail(5))