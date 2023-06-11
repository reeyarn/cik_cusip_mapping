# cik_cusip_mapping

Forked from [leoliu0/cik-cusip-mapping](https://github.com/leoliu0/cik-cusip-mapping)

MIT License
Reeyarn Li 2023


## Originally Stated Purpose by [leoliu0/cik-cusip-mapping](https://github.com/leoliu0/cik-cusip-mapping)

*** If you just want the mapping, download cik-cusip-maps.csv ***

This repository produces the link between cik and cusip using EDGAR 13D and 13G fillings, that is more robust than Compustat (due to backward filling of new cusip to old records). It is a competitor to WRDS SEC platform while this one is free.

This is written in python36+, I don't provide a requirement file and I only use very common libraries, if you run into Module not Found problem, just pip install them, e.g. Pandas

dl_idx.py will download the EDGAR index file containing addresses for each filing, i.e. full_index.csv is generated

```
python dl_idx.py
```

## Forked by Reeyarn Li:

Assume that an issuer does not change its Issuer Number (first 6 chars of CUSIP) when the issuer's name does not change, 
one does not need to download every 13D and 13G filings. Just download those at the first and last date with cik x company-name pair.

`python3 build_cik_cusip_link.py`

This code replaces `parse_cusip_html.py` by merging `dl.py` download function and `parse_cusip_html.py` function.
In addition, it runs `pd.groupby(["cik", "comnam"])` to get the min and max filing `date` for each cik x company-name pair, 
and obtain the CUSIP6 for the first and last 13D/G filing during the window.



The output csv from leoliu0/cik-cusip-mapping assumes that cik--cusip link does not change overtime. See his explanation below. 
But in my case, I need to build cik--name--cusip6---begdate--enddate structure, to be merged with Compustat/CRSP with CUSIP6 and date range.
For this purpose, my output looks like the following:

`finalset.loc[finalset.cik==772263, ["cik", "comnam", "cusip6", "strdate_subset", "enddate_subset", "strdate", "enddate"]]`

```
"""
          cik                comnam  cusip6 strdate_subset enddate_subset    strdate    enddate
24310  772263  BEEBAS CREATIONS INC  076590     1994-04-08     1994-06-10 1994-04-08 1995-11-03
72845  772263           NITCHES INC  65476M     1998-02-10     2007-01-10 1996-11-20 2008-11-26
"""
```

The other files are not touched for now.

## Original Functions from  leoliu0/cik-cusip-mapping

dl.py will download a certain type of filing, check form_type.txt for available filing types. for example,
```python
python dl.py 13G 13G # this will download all 13G (second 13G) filing into 13G (first 13G) folder
```
```python
python parse_cusip.py 13G # this will process all files in 13G directory, creating a file called 13G.csv with filing name, cik, cusip number.
```
Finally, you can clean the resulting csv files and get the mapping
```python
python post_proc.py 13G.csv 13D.csv
# This will process both 13G.csv and 13D.csv and generate the mapping file
```

If you do not care obtaining the original data, just download cik-cusip-maps.csv, it has the mapping without timestamp information, but should be good if you use it for merging databases. Please deal with duplications yourself.

The reason why I do not provide timestamp is because there will be truncations due to timing of the filings. For example, when filings are filed in 2005 and 2007 for a link, I can only see the link in 2005 and 2007, but the link should be valid in 2006 too. One way to fix this is to interpolate the link to 2006. However, when filing ends in 2006, we do not know when should the link is valid to and how long after we should extrapolate, i.e. One could extrapolate to 2020 but we do not know the true date the link ends. This is arbitrary choice of the user, therefore I remove the timestamp for you to deal with yourself. For database merging purpose, this should be fine because two databases you are merging should have timestamp and it's rare for duplicated links to exist at some given time.

*** Finally, if you find this repo useful, please give it a STAR ***
