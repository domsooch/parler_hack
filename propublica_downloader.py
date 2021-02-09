import sys, os, json, re, copy

from importlib import reload
import os, sys, json, glob
import sys, os, random, xmltodict
import pandas as pd
import datetime
import re, datetime, fnmatch
from bs4 import BeautifulSoup as BS
import requests
from tqdm import tqdm

import utils.out_path as OP




def writeFails(t):
    with open(fail_path, "a") as myfile:
        myfile.write("%s\t%s\n"%(OP.TimeCode(), t))
        
if __name__=='__main__':
    ud="/tdata0/parler_propublica/"
    fail_path=ud+'FailedDownloads.txt'
    
    if not(os.path.exists(ud)):
        os.mkdir(ud)
        
    with open(ud+'out_videoz.json', 'r') as f:
        jlst=json.loads(f.read())

    i=0
    for j in jlst:
        image_url = j['@data-src']
        opth = ud+image_url.split('/')[-1]
        if os.path.exists(opth):
            print('skipping: %i %s'%(i, opth))
        print('%i of %i %s'%(i, len(jlst), opth))

        # Streaming, so we can iterate over the response.
        response = requests.get(image_url, stream=True)
        total_size_in_bytes= int(response.headers.get('content-length', 0))

        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
        with open(opth, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong", opth)
            writeFails('%i\t%s'%(i,image_url))
        i+=1