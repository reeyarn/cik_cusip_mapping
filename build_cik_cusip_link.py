"""
build_cik_cusip_link.py

This code replaces parse_cusip_html.py by merging dl.py download function and parse_cusip_html function.
In addition, you don't need to download all 13D 13G filings. Just those at the first and last date with cik x company-name pair,
assuming CUSIP-6 Issuer Number remain unchanged until company name change.

MIT License

Reeyarn Li 2023
"""


import os
import re 
import time
from collections import Counter


from pathlib import Path

import requests
import pandas as pd
import gzip 

#pip install mapply
import mapply    

mapply.init(
    n_workers=10,
    chunk_size=100,
    max_chunks_per_worker=100,
    progressbar=True
)


cleanhtml = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
html_junk = re.compile(r'''["].*["]|=#.*\d+''')

pat = re.compile(
    '[\( >]*[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[- ]*[0-9]{0,2}[- ]*[0-9]{0,1}[\) \n<]*'
)

w = re.compile('\w+')

import datetime
import time 

def _get_secondstamp():
    today = datetime.datetime.utcnow().date()
    start_of_day = datetime.datetime.combine(today, datetime.time.min)
    now_ms = round((time.time() - start_of_day.timestamp())*1000)
    return now_ms

def _check_if_need_sleep(_last_sec_download_time = 0):
    REQUEST_BUDGET_MS = 200
    need_sleep_for_ms = 0
    _tmpfilename_last_sec_download_time = "/tmp/_tmpfilename_last_sec_download_time"
    if os.path.exists(_tmpfilename_last_sec_download_time):
        with open(_tmpfilename_last_sec_download_time, "r") as file:
            last_request_time = file.read()
        try:
            _tmp = int(last_request_time)   
        except:
            print(f"From log file read time {last_request_time} but cannot do int(); sleep 1s")
            time.sleep(1)
        #self.logger.debug(f"Read from lock file last time stamp : {str(self._last_sec_download_time)}") 
    else:
        if _last_sec_download_time == 0:
           _last_sec_download_time = _get_secondstamp()    
    
    elapsed =  _get_secondstamp() - _last_sec_download_time
    if elapsed < REQUEST_BUDGET_MS:
        need_sleep_for_ms = REQUEST_BUDGET_MS-elapsed
    return need_sleep_for_ms


USERAGENT="University of YourUniv apk03@student.uni-youruniv.edu"



def get_cusip(url):
    folder = "/text/edgar/13D/"
    cik = url.split("/")[-2]
    tfnm = url.split(".")[0].split("/")[-1]
    Path(f"/{folder}/{cik}/{tfnm}/").mkdir(parents=True, exist_ok=True)
    file_path = f"/{folder}/{cik}/{tfnm}/{tfnm}.txt.gz"
    raw = None
    if os.path.exists(file_path):
        with open(file_path, 'rb') as f:
            raw = f.read()
        raw = gzip.decompress(raw)     
        try:
            raw = raw.decode() 
        except:
            try:
                raw = raw.decode('latin-1')    
            except:
                raw = bytes()
        
        if not "SEC-DOCUMENT" in raw and not "SEC-HEADER" in raw and not "DOCUMENT" in raw:
            print(f"delete illegal file for {url}", raw[:200])
            os.unlink(file_path)
        else:   
            #print(f"Use existing file for {url}") 
            pass
        
    if not os.path.exists(file_path):
        need_sleep_for_ms = 700
        _tmpfilename_last_sec_download_time = "/tmp/_tmpfilename_last_sec_download_time"
        while need_sleep_for_ms > 0:
            time.sleep(need_sleep_for_ms/1000)
            need_sleep_for_ms = _check_if_need_sleep()
            if need_sleep_for_ms >3 and need_sleep_for_ms<10000:
                time.sleep(min(0.1, need_sleep_for_ms/1000))
                need_sleep_for_ms = _check_if_need_sleep()       
            else:
                need_sleep_for_ms =0
                
        #Write requset starting time to lock file    
        _last_sec_download_time = _get_secondstamp()
        with open(_tmpfilename_last_sec_download_time, "w") as file:
            file.write(str(_last_sec_download_time) )
        
        print(str(time.time()), f"download file from {url}")
        req = requests.get(f"https://www.sec.gov/Archives/{url}", headers={'User-Agent': USERAGENT}, stream=False)
        req_status = str(req.status_code)
        if req_status == "200":    
            txt = req.content
            try:
                raw = txt.decode()
            except UnicodeDecodeError:
                try:
                    raw = txt.decode('latin-1')    
                except:
                    raw = bytes()
            data_gzipped = gzip.compress(txt) 		
            with open(file_path, "wb") as f:
                f.write(data_gzipped)       
        elif req_status.startswith ("404"):
            print(f"{cik} {tfnm} 404 failed to download")     
            return None                
        elif req_status.startswith ("429") or req_status.startswith ("5"):
            print(f"Cannot get url {url} due to permanent error: {req_status} too many requst 429. sleep for 10 minute 30s")
            # Sleep for 10 minutes (600 seconds)
            time.sleep(630)
            try:
                req = requests.get(f"https://www.sec.gov/Archives/{url}", headers={'User-Agent': USERAGENT}, stream=False)
                txt = req.content
                data_gzipped = gzip.compress(txt) 		
                with open(file_path, "wb") as f:
                    f.write(data_gzipped)       
            except:
                print(f"{cik} {tfnm} failed to download")                     
                return None
    
    if not os.path.exists(file_path):
        return None
    if raw == None:
        with open(file_path, 'rb') as f:
            raw = f.read()
        raw = gzip.decompress(raw)     
        raw = raw.decode() 
    raw = raw.replace(r"<DOCUMENT>", r"***starter***")
    lines = cleanhtml.sub('\n', raw).split('\n')
    
    record = 0
    cik = None
    for line in lines:
        if 'SUBJECT COMPANY' in line:
            record = 1
        if 'CENTRAL INDEX KEY' in line and record == 1:
            cik = line.split('\t\t\t')[-1].strip()
            break
    
    cusips = []
    record = 0
    
    for line in lines:
        if '***starter***' in line:  # lines are after the document preamble
            #print('****************************************')
            record = 1
        if record == 1:
            line = html_junk.sub('', line)
            line = cleanhtml.sub('', line)
            if 'IRS' not in line and 'I.R.S' not in line:
                fd = pat.findall(line)
                if fd:
                    #if args.debug:
                    #    print(f'FOUND: {fd} from {line}')
                    cusip = fd[0].strip().strip('<>')
                    cusips.append(cusip)
    if len(cusips) == 0:
        cusip = None
    else:
        cusip = Counter(cusips).most_common()[0][0]
        cusip = ''.join(w.findall(cusip))
    #if args.debug:
    #    print(cusip)
    
    return cusip






  

if __name__=="__main__":  
    index = pd.read_csv(os.path.expanduser("./full_index.csv"))
    
    bigset = index.loc[ (index.form.str.contains("13D")) | (index.form.str.contains("13G"))| (index.form.str.contains("10-K"))  ].reset_index()
    bigset["date"] =  pd.to_datetime( bigset["date"])
    
    bigset["strdate"] = bigset.groupby(["cik", "comnam"]).date.transform("min")
    bigset["enddate"] = bigset.groupby(["cik", "comnam"]).date.transform("max")
    
    bigset = bigset.loc[(bigset.date ==bigset.strdate) | (bigset.date==bigset.enddate)].reset_index(drop=True)

    
    subset = index.loc[ (index.form.str.contains("13D")) | (index.form.str.contains("13G"))| (index.form.str.contains("10-K"))  ].reset_index()
    subset["date"] =  pd.to_datetime( subset["date"])
    
    subset["strdate"] = subset.groupby(["cik", "comnam"]).date.transform("min")
    subset["enddate"] = subset.groupby(["cik", "comnam"]).date.transform("max")
    
    subset = subset.loc[(subset.date ==subset.strdate) | (subset.date==subset.enddate)].reset_index(drop=True)

    
    subset["cusip"] = subset.url.mapply(get_cusip)
    subset["cusip6"] = subset["cusip"].str[:6]
    
    extendset = pd.merge(bigset[["cik", "comnam", "strdate", "enddate"]], subset[["cik", "comnam", "strdate", "enddate", "cusip", "cusip6"]], on=["cik", "comnam"], how="left", suffixes=('_bigset', '_subset'))
    
    extendset["strdate"] = extendset.groupby(["cik", "comnam", "cusip6"]).strdate_bigset.transform("min")
    extendset["enddate"] = extendset.groupby(["cik", "comnam", "cusip6"]).enddate_bigset.transform("max")

    
    extendset = extendset.drop_duplicates(subset=["cik", "comnam", "cusip6"])
    #162992
    
    #extendset.drop_duplicates(subset=["cik", "comnam"])    #122629
    #It means for each cik -- comnam, there are more than one cusip6 found. 
    #Can only be resolved after merging with another data source, such as comp.funda
    #then try to compare company name
    
    extendset["n_cusips"] = extendset.groupby(["cik", "comnam"]).cusip.transform("count")
    extendset.loc[extendset.n_cusips>1]


    #For each CIK, how many form 10-Ks are filed during the window?
    tenkset = index.loc[ (index.form.str.contains("10-K"))  ].reset_index()
    tenkset["date"] =  pd.to_datetime( tenkset["date"])
    
    tenkset["strdate"] = tenkset.groupby(["cik", "comnam"]).date.transform("min")
    tenkset["enddate"] = tenkset.groupby(["cik", "comnam"]).date.transform("max")
    
    tenkset["num10ks"] = tenkset.groupby(["cik", "comnam"]).url.transform("count")
    
    tenkset = tenkset.loc[(tenkset.date ==tenkset.strdate) | (tenkset.date==tenkset.enddate)].reset_index(drop=True)

    extendset1 = pd.merge(extendset, tenkset[["cik", "comnam", "num10ks"]], on=["cik", "comnam"], how="left")
    #212_648
    extendset1.num10ks.fillna(0, inplace=True)
    
    #extendset2 = extendset1.loc[extendset1.groupby(["cik", "comnam"]).num10ks.idxmax()]
    
    finalset = extendset1.reset_index(drop=True)
    
    finalset.to_csv("cik_cusips.csv", index=False, sep="|")
    
if False: # the expected outputs:
    subset.loc[subset.cik==772263,["cik", "comnam", "cusip6"]]
    """
           cik                comnam  cusip6
3787    772263  BEEBAS CREATIONS INC  076590
3788    772263  BEEBAS CREATIONS INC  076590
31022   772263           NITCHES INC  65476M
103963  772263           NITCHES INC  65476M
    """
    
    finalset.loc[finalset.cik==772263]
    """
         cik                comnam strdate_bigset enddate_bigset strdate_subset enddate_subset      cusip  cusip6    strdate    enddate  n_cusips  num10ks
8356  772263  BEEBAS CREATIONS INC     1994-04-08     1995-11-03     1994-04-08     1994-06-10  076590108  076590 1994-04-08 1995-11-03         1      1.0
8357  772263           NITCHES INC     1996-11-20     2008-11-26     1998-02-10     2007-01-10  65476M109  65476M 1996-11-20 2008-11-26         1     27.0
    """
    
  
