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
    REQUEST_BUDGET_MS = 116
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
        raw = raw.decode() 
        if not "SEC-DOCUMENT" in raw and not "SEC-HEADER" in raw and not "DOCUMENT" in raw:
            print(f"delete illegal file for {url}", raw[:200])
            os.unlink(file_path)
        else:   
            print(f"Use existing file for {url}") 
        
    if not os.path.exists(file_path):
        need_sleep_for_ms = 500
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
            raw = txt.decode()
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

    index = index.loc[ (index.form.str.contains("13D")) | (index.form.str.contains("13G"))  ].reset_index()
    index["date"] =  pd.to_datetime( index["date"])

    index["strdate"] = index.groupby(["cik", "comnam"]).date.transform("min")
    index["enddate"] = index.groupby(["cik", "comnam"]).date.transform("max")
    
    subset = index.loc[(index.date ==index.strdate) | (index.date==index.enddate)].reset_index(drop=True)
    
    subset["cusip"] = subset.url.mapply(get_cusip)
    subset.to_csv("cik_cusips.csv", index=False, sep="|")


  
