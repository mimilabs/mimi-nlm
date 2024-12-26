# Databricks notebook source
!pip install tqdm

# COMMAND ----------

import requests
from time import sleep
from typing import Dict, Any, Optional
from dateutil.parser import parse
from tqdm import tqdm
import pandas as pd
from datetime import datetime

# COMMAND ----------

def fetch_rxnorm_ndc(rxcui: str, 
                     max_retries: int = 3, 
                     rate_limit_delay: float = 0.1) -> Optional[Dict[str, Any]]:
    """
    Fetch historical NDCs for a given RxCUI with retry and rate limiting.
    
    Args:
        rxcui: RxCUI identifier
        max_retries: Maximum number of retry attempts
        rate_limit_delay: Delay between API calls in seconds
    
    Returns:
        API response as dictionary or None if all retries failed
    """
    url = f'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json'
    
    for attempt in range(max_retries):
        try:
            # Rate limiting delay
            if attempt > 0:
                sleep(rate_limit_delay)
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Error fetching RxCUI {rxcui} after {max_retries} attempts: {str(e)}")
                return None
            
            # Exponential backoff
            sleep_time = 2 ** attempt
            print(f"Attempt {attempt + 1} failed, retrying in {sleep_time} seconds...")
            sleep(sleep_time)
    
    return None

def flatten_historical_ndcs(api_response, query_rxcui):
    flattened_records = []
    historical_ndc_records = (api_response.get('historicalNdcConcept', {})
                                .get('historicalNdcTime', []))
    
    for ndc_association in historical_ndc_records:
        association_type = ndc_association.get('status')  # 'direct' vs 'indirect' association
        concept_rxcui = ndc_association.get('rxcui')      # RxCUI that NDCs were directly linked to
        
        for release_period in ndc_association.get('ndcTime', []):
            first_release = parse(release_period.get('startDate')+'01').date()  # First RxNorm release date
            last_release = parse(release_period.get('endDate')+'01').date()     # Last RxNorm release date
            
            for ndc_code in release_period.get('ndc', []):
                flattened_records.append([
                    query_rxcui,       # Original RxCUI we queried for
                    concept_rxcui,     # RxCUI of concept to which the NDCs are or were directly associated
                    ndc_code,          # The NDC code itself
                    association_type,  # "direct" = NDC is associated with the subject RxCUI. "indirect" = NDC was inherited through remapping
                    first_release,     # the first RxNorm release where the NDC was active for this concept. Format is YYYYMM
                    last_release       # the last RxNorm release where the NDC was active for this concept. Format is YYYYMM
                ])
                
    return flattened_records

# COMMAND ----------

pdf = spark.sql("""select distinct rxcui from mimi_ws_1.nlm.rxnconso where tty in ('SCD','SBD','GPCK','BPCK') and sab = 'RXNORM'""").toPandas()

# COMMAND ----------

header = ["query_rxcui",
            "concept_rxcui",
            "ndc_code",
            "association_type",
            "first_release",
            "last_release"]

# COMMAND ----------

data = []
mode = "overwrite"
for rxcui in tqdm(pdf['rxcui']):
    res = fetch_rxnorm_ndc(rxcui)
    if res is None:
        continue
    data.extend(flatten_historical_ndcs(res, rxcui))
    

    if len(data) > 10000:
        pdf = pd.DataFrame(data, columns=header)
        pdf['mimi_src_file_date'] = datetime.today().date()
        pdf['mimi_src_file_name'] = 'https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/allhistoricalndcs.json'
        pdf['mimi_dlt_load_date'] = datetime.today().date()
        df = spark.createDataFrame(pdf)
        (
            df.write.mode(mode)
                .saveAsTable('mimi_ws_1.nlm.historical_ndcs')
        )    
        data = []
        mode = 'append'

# COMMAND ----------


