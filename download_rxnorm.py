# Databricks notebook source
# MAGIC %run /Workspace/Repos/yubin.park@mimilabs.ai/mimi-common-utils/download_utils

# COMMAND ----------

!pip install beautifulsoup4 tqdm

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import re

url_base = "https://www.nlm.nih.gov"
volumepath = "/Volumes/mimi_ws_1/nlm/src/rxnorm/zipfiles/"

# COMMAND ----------

page = "/research/umls/rxnorm/docs/rxnormfiles.html"
response = requests.get(f"{url_base}{page}")
response.raise_for_status()  # This will raise an error if the fetch fails
soup = BeautifulSoup(response.text, 'html.parser')

# COMMAND ----------

urls = []
filenames = []
for a in soup.find_all('a', href=True):
    if a['href'].endswith('.zip') and '_prescribe_' in a.text:
        urls.append(a['href'])

# COMMAND ----------

download_files(urls, volumepath)

# COMMAND ----------

for path_zip in Path(volumepath).glob('*.zip'):
    path_unzip = str(path_zip.parents[1]) + '/' + str(path_zip.stem)
    unzip(path_zip, path_unzip)

# COMMAND ----------


