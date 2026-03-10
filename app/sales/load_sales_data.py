from supabase import create_client, Client
import pandas as pd
from dotenv import load_dotenv
import os


# Read .env
load_dotenv()


# url
url = os.getenv("SUPABASE_URL")
# key
key = os.getenv("SUPABASE_KEY")

# connect to superbase project with Client
supabase: Client = create_client(url, key)

# read the cleaned sales data
df = pd.read_csv("csv_cleaned_file", sep="|")


records = df.to_dict(orient="records")
supabase.table("taable_name").insert(records).execute()
