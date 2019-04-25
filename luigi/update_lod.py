# usage for debug:
# PYTHONPATH="$PYTHONPATH:." luigi --module update_lod LODUpdate --local-scheduler

import json
import sys
from datetime import datetime,date,timedelta
from dateutil import parser
from requests import get,head,put
from time import sleep
import os
import shutil
from gzip import decompress
import subprocess
import argparse

import elasticsearch
from multiprocessing import Pool,current_process
from pyld import jsonld
import ijson.backends.yajl2_cffi as ijson
from es2json import put_dict, esidfilegenerator

import luigi
import luigi.contrib.esindex
from gluish.task import BaseTask,ClosestDateParameter
from gluish.utils import shellout

class LODTask(BaseTask):
    """
    Just a base class for LOD
    """
    with open('lod_config.json') as data_file:    
        config = json.load(data_file)
    r=get("{host}/date/actual/1".format(**config))
    lu=r.json().get("_source").get("date")
    config["lastupdate"]=date(int(lu.split('-')[0]),int(lu.split('-')[1]),int(lu.split('-')[2]))
    PPNs=[]
    TAG = 'lod'
    yesterday = date.today() - timedelta(1)
    span = yesterday-config.get("lastupdate")
    for i in range(span.days+1):
        date=(config.get("lastupdate")+timedelta(days=i)).strftime("%y%m%d")
        if os.path.isfile("TA-MARC-norm-{date}.tar.gz".format(date=date)):
            continue
        config["dates"].append(date)
        
    def closest(self):
        return daily(date=self.date)

class LODDownload(LODTask):

    def run(self):
        for n,dat in enumerate(self.config.get("dates")):
            cmdstring="wget --user {username} --password {password} {url}TA-MARC-norm-{date}.tar.gz".format(**self.config,date=dat)
            try:
                output = shellout(cmdstring)
            except Exception as e:
                continue
        return 0

    def complete(self):
        for n,date in enumerate(self.config.get("dates")):
            if not os.path.exists("TA-MARC-norm-{date}.tar.gz".format(**self.config,date=date)):
                return False
        return True

class LODExtract(LODTask):
    
    def requires(self):
        return LODDownload()
    
    def run(self):
        for date in self.config.get("dates"):
            cmdstring="tar xvzf TA-MARC-norm-{date}.tar.gz && cat norm-aut.mrc >> {yesterday}-norm.mrc && rm norm-*.mrc".format(**self.config,date=date,yesterday=self.yesterday.strftime("%y%m%d"))
            output = shellout(cmdstring)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-norm.mrc".format(date=self.yesterday.strftime("%y%m%d")))

class LODTransform2ldj(LODTask):
    
    def requires(self):
        return LODExtract()

    def run(self):
        cmdstring="marc2jsonl < {date}-norm.mrc | ~/git/efre-lod-elasticsearch-tools/helperscripts/fix_mrc_id.py > {date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmdstring)
        with open("{date}-norm-aut-ppns.txt".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"w") as outp,open("{date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"r") as inp:
            for rec in inp:
                print(json.loads(rec).get("001"),file=outp)
        return 0
    
    def output(self):
        return luigi.LocalTarget("{date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d")))


class LODFillRawdataIndex(LODTask):
    """
    Loads raw data into a given ElasticSearch index (with help of esbulk)
    """
    
    def requires(self):
        return LODTransform2ldj()
    
    def run(self):
        #put_dict("{host}/swb-aut".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"mappings":{"mrc":{"date_detection":False}}})
        #put_dict("{host}/swb-aut/_settings".format(**self.config,date=self.yesterday.strftime("%y%m%d")),{"index.mapping.total_fields.limit":5000})

        cmd="esbulk -verbose -server {host} -w {workers} -index swb-aut -type mrc -id 001 {date}-norm-aut.ldj""".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)

    def complete(self):
        fail=0
        es_recordcount=0
        file_recordcount=0
        es_ids=set()
        for record in esidfilegenerator(host="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[0],port="{host}".format(**self.config).rsplit("/")[2].rsplit(":")[1],index="swb-aut",type="mrc",idfile="{date}-norm-aut-ppns.txt".format(**self.config,date=self.yesterday.strftime("%y%m%d")),source="False"):
            es_ids.add(record.get("_id"))
        es_recordcount=len(es_ids)
        
        try:
            with open("{date}-norm-aut.ldj".format(**self.config,date=self.yesterday.strftime("%y%m%d")),"r") as fd:
                ids=set()
                for line in fd:
                    jline=json.loads(line)
                    ids.add(jline.get("001"))
            file_recordcount=len(ids)
            print(file_recordcount)
        except FileNotFoundError:
            return False
        
        if es_recordcount==file_recordcount and es_recordcount>0:
            return True
        return False

class LODProcessFromRdi(LODTask):
    def requires(self):
        return LODFillRawdataIndex()
    
    def run(self):
        cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && ~/git/efre-lod-elasticsearch-tools/processing/esmarc.py -z -server {host}/swb-aut/mrc -idfile {date}-norm-aut-ppns.txt -prefix {date}-aut-data".format(**self.config,date=self.yesterday.strftime("%y%m%d"))
        output=shellout(cmd)
        sleep(5)
        
    def complete(self):
        returnarray=[]
        path="{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        try:
            for index in os.listdir(path):
                for f in os.listdir(path+"/"+index):
                    if not os.path.isfile(path+"/"+index+"/"+f):
                        return False
        except FileNotFoundError:
            return False
        return True
    
class LODUpdate(LODTask):
    def requires(self):
        return LODProcessFromRdi()
    
    def run(self):
        path="{date}-aut-data".format(date=self.yesterday.strftime("%y%m%d"))
        enrichmentstr=[]
        for index in os.listdir(path):
            for f in os.listdir(path+"/"+index):                                        ### doing several enrichment things before indexing the data
                cmd=". ~/git/efre-lod-elasticsearch-tools/init_environment.sh && zcat {fd} | ".format(fd=path+"/"+index+"/"+f)#with -pipeline, all the data get's thru, not only enriched docs
                #cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/sameAs2id.py         -pipeline -stdin -searchserver {host}  | ".format(**self.config)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/entityfacts-bot.py   -pipeline -stdin -searchserver {host} | ".format(**self.config)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/gnd-sachgruppen.py   -pipeline -searchserver {host}  | ".format(**self.config)
                cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/wikidata.py          -pipeline -stdin | "
                if index=="geo":
                    cmd+="~/git/efre-lod-elasticsearch-tools/enrichment/geonames.py       -pipeline -stdin -searchserver {geonames_host} | ".format(**self.config)
                cmd+="esbulk -verbose -server {host} -w 1 -size 20 -index {index} -type schemaorg -id identifier".format(**self.config,index=index)
                output=shellout(cmd)
        put_dict("{host}/date/actual/1".format(**self.config),{"date":str(self.yesterday.strftime("%Y-%m-%d"))})
    
    def output(self):
        return luigi.LocalTarget(path=self.path())


    def complete(self):
        yesterday = date.today() - timedelta(1)
        now=yesterday.strftime("%Y-%m-%d")
        r=get("{host}/date/actual/1".format(**self.config))
        lu=r.json().get("_source").get("date")
        if lu==now:
            return True
        else:
            return False
        
