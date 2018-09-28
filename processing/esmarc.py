#!/usr/bin/python3
# -*- coding: utf-8 -*-
from rdflib import URIRef
import traceback
from uuid import uuid4
from multiprocessing import Pool, Manager,current_process,Process,cpu_count
import elasticsearch
import json
#import urllib.request
import codecs
import argparse
import sys
import io
import os.path
import mmap
import requests
import siphash
import re
from es2json import esgenerator
from es2json import esfatgenerator
from es2json import ArrayOrSingleValue
from es2json import eprint
from es2json import litter


def isint(num):
    try: 
        int(num)
        return True
    except:
        return False
    
def getiso8601(date):
    p=re.compile(r'[\d|X].\.[\d|X].\.[\d|X]*') #test if D(D).M(M).Y(YYY)
    m=p.match(date)
    datestring=""
    if m:
        slices=list(reversed(date.split('.')))
        if isint(slices[0]):
            datestring+=str(slices[0])
        for slice in slices[1:]:
            if isint(slice):
                datestring+="-"+str(slice)
            else:
                break
        return datestring
    else:
        return date #was worth a try
    
def dateToEvent(date,schemakey):
    if '-' in date:
        dates=date.split('-')
        if date[0]=='[' and date[-1]==']': #oh..
            return str("["+dateToEvent(date[1:-1],schemakey)+"]")        
        if "irth" in schemakey: # (date of d|D)eath(Dates)
                return getiso8601(dates[0])
        elif "eath" in schemakey: #(date of d|D)eath(Date)
            if len(dates)==2:
                return getiso8601(dates[1])
            elif len(dates)==1: 
                return None # still alive! congrats
        else:
            return date

def handlesex(record,key,entity):
    for v in key:
        marcvalue=getmarc(v,record,entity)
        if isinstance(marcvalue,list):
            marcvalue=marcvalue[0]
    if isint(marcvalue):
        marcvalue=int(marcvalue)
    if marcvalue==0:
        return "Unknown"
    elif marcvalue==1:
        return "Male"
    elif marcvalue==2:
        return "Female"
    elif marcvalue==9:
        return None


isil2sameAs = {
    "DE-576":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    "DE-588":"http://d-nb.info/gnd/",
    "DE-601":"http://gso.gbv.de/PPN?PPN=",
    "(DE-576)":"http://swb.bsz-bw.de/DB=2.1/PPNSET?PPN=",
    "(DE-588)":"http://d-nb.info/gnd/",
    "(DE-601)":"http://gso.gbv.de/PPN?PPN="
}

marc2relation = {
    "VD-16 Mitverf": "contributor",
    "v:Mitverf": "contributor",
    "v:Co-Autor": "contributor",
    "v:Doktorvater": "contributor",
    "v:Illustrator": "contributor",
    "v:Übersetzer": "contributor",
    "v:Biograf": "contributor",
    "v:Förderer": "contributor",
    "v:Berater": "contributor",
    "v:hrsg": "contributor",
    "v:Mitautor": "contributor",
    "v:Partner": "contributor",
    
    "v:Tocher": "children",            #several typos in SWB dataset
    "tochter": "children",
    "Z:Tochter":"children",
    "Sohn": "children",
    "v:Nachkomme": "children",
    "v:zweites Kind": "children",
    
    "v:Gattin": "spouse",
    "v:Gatte": "spouse",
    "v:Gemahl": "spouse",
    "Ehe": "spouse",
    "Frau": "spouse",
    "Mann": "spouse",
    
    "v:Jüngster Bruder": "sibling",
    "Bruder": "sibling",
    "v:Zwilling": "sibling",
    "Schwester": "sibling",
    "v:Halbschwester": "sibling",
    "v:Halbbruder": "sibling",
    "Vater": "parent",
    "Z:Vater":"parent",
    "v:Stiefvater": "parent",
    "Mutter": "parent",
    
    "Nachfolger": "follows",
    "Vorgänger": "follows",
    
    "v:Vorfahr": "relatedTo",
        
    "v:Schüler": "relatedTo",        
    "Lehrer": "relatedTo",        
    "v:frühere Ehefrau": "relatedTo",
    "v:Schwager": "relatedTo",        
    "v:Ur": "relatedTo",          
    "v:Muse": "relatedTo",
    "v:Nachfahre": "relatedTo",         
    "v:Groß": "relatedTo",    
    "v:Langjähriger Geliebter": "relatedTo",    
    "v:Lebensgefährt": "relatedTo",    
    "v:Nichte": "relatedTo",
    "v:Stiefnichte": "relatedTo",
    "v:Neffe": "relatedTo",
    "v:Onkel": "relatedTo",
    "v:Tante": "relatedTo",
    "v:Verlobt": "relatedTo",
    "v:Vorfahren": "relatedTo",
    "v:Vetter": "relatedTo",
    "v:Tauf": "relatedTo",
    "v:Pate": "relatedTo",
    "v:Schwägerin": "relatedTo",
    "v:Schwiegervater": "relatedTo",
    "v:Schwiegermutter": "relatedTo",
    "v:Schwiegertochter": "relatedTo",
    "v:Schwiegersohn": "relatedTo",
    "v:Enkel": "relatedTo",
    "v:Mätresse": "relatedTo",
    "Freund": "knows",
    "v:Großvater": "relatedTo",
    "v:Cousin": "relatedTo",
    "v:Lebenspartner": "relatedTo",
    "v:Berater und Freund": "relatedTo",
    "v:Geliebte": "relatedTo",
    "v:Modell und Lebensgefährtin":"relatedTo",
    "v:Liebesbeziehung":"relatedTo",
    
    "v:publizistische Zusammenarbeit und gemeinsame öffentliche Auftritte": "colleague",
    "v:Sekretär": "colleague", 
    "v:Privatsekretär": "colleague", 
    "v:Kolleg": "colleague", 
    "v:Mitarbeiter": "colleague", 
    "v:Kommilitone": "colleague", 
    "v:Zusammenarbeit mit": "colleague", 
    "v:gemeinsames Atelier": "colleague", 
    "v:Geschäftspartner": "colleague" , 
    "v:musik. Partnerin": "colleague" ,
    "v:Künstler. Partner": "colleague" ,  
    "assistent": "colleague",
    
    #see also http://www.dnb.de/SharedDocs/Downloads/DE/DNB/standardisierung/inhaltserschliessung/gndCodes.pdf?__blob=publicationFile
    "4:adel":"honorificPrefix",
    "4:adre":"recipent",
    "4:adue":"parentOrganization",
    "4:affi":"affiliation",
    #"4:akad":"honorificPrefix",
    "4:akti":"hasProfession",
    "4:bezf":"relatedTo",
    "4:bezb":"colleague",
    "4:beza":"knows",
    "4:bete": "contributor",
    "4:rela":"knows",
    "4:affi":"knows",
    "4:aut1":"author",
}

def gnd2uri(string):
    try:
        if isinstance(string,list):
            for n,uri in enumerate(string):
                string[n]=gnd2uri(uri)
            return string
        if string and "(DE-" in string:
            if isinstance(string,list):
                ret=[]
                for st in string:
                    ret.append(gnd2uri(st))
                return ret
            elif isinstance(string,str):
                return uri2url(string.split(')')[0][1:],string.split(')')[1])
    except:
        return 

def uri2url(isil,num):
    if isil and num and isil in isil2sameAs:
        return str(isil2sameAs.get(isil)+num)
    else:
        return str("("+isil+")"+num)    #bugfix for isil not be able to resolve for sameAs, so we give out the identifier-number

def id2uri(string,entity):
    return "http://data.slub-dresden.de/"+entity+"/"+string
    
def get_or_generate_id(record,entity):
    generate= False
    #set generate to True if you're 1st time filling a infrastructure from scratch!
    # replace args.host with your adlookup service :P
    if generate:
        identifier = None
    else:
        if record.get("003") and record.get("001"):
            ppn=gnd2uri("("+str(getmarc(record,"003",entity)+")"+str(getmarc(record,"001",entity))))
            if ppn:
                url="http://localhost:9200/"+entity+"/schemaorg/_search?q=sameAs:\""+ppn+"\""
                try:
                    r=requests.get(url)
                    if r.json().get("hits").get("total")>=1:
                        identifier=r.json().get("hits").get("hits")[0].get("_id")
                    else:
                        identifier=None
                except Exception as e:
                    with open("errors.txt",'a') as f:
                        traceback.print_exc(file=f)
                    identifier=None
            else:
                identifier=None
        else:
            identifier=None
        
    if identifier:
        return id2uri(identifier,entity)
    else:
        return id2uri(siphash.SipHash_2_4(b'slub-dresden.de/').update(uuid4().bytes).hexdigest().decode('utf-8').upper(),entity)

def getmarc(record,regex,entity):
    if "+" in regex:
        marcfield=regex[:3]
        if marcfield in record:
            subfields=regex.split(".")[-1].split("+")
            data=None
            for array in record.get(marcfield):
                for k,v in array.items():
                    sset={}
                    for subfield in v:
                        for subfield_code in subfield:
                            sset[subfield_code]=subfield[subfield_code]
                    fullstr=""
                    for sf in subfields:
                        if sf in sset:
                            if fullstr:
                                fullstr+=". "
                            if isinstance(sset[sf],str):
                                fullstr+=sset[sf]
                            elif isinstance(sset[sf],list):
                                fullstr+=". ".join(sset[sf])
                    if fullstr:
                        data=litter(data,fullstr)
            if data:
                return ArrayOrSingleValue(data)
    else:
        ret=[]
        if isinstance(regex,str):
            regex=[regex]
        for string in regex:
            if string[:3] in record:
                ret=litter(ret,ArrayOrSingleValue(list(getmarcvalues(record,string,entity))))
        if ret:
            if isinstance(ret,list):    #simple deduplizierung via transformation zum set und retour
                ret = list(set(ret))
            return ArrayOrSingleValue(ret)

def getmarcvalues(record,regex,entity):
        if len(regex)==3 and regex in record:
            yield record.get(regex)
        #eprint(regex+":\n"+str(record)+"\n\n") ### beware! hardcoded traverse algorithm for marcXchange record encoded data !!! ### temporary workaround: http://www.smart-jokes.org/programmers-say-vs-what-they-mean.html
        else:
            record=record.get(regex[:3]) # = [{'__': [{'a': 'g'}, {'b': 'n'}, {'c': 'i'}, {'q': 'f'}]}]
            if isinstance(record,list):  
                for elem in record:
                    if isinstance(elem,dict):
                        for k in elem:
                            if isinstance(elem[k],list):
                                for final in elem[k]:
                                    if regex[-1] in final:
                                        yield final.get(regex[-1])

def handle_rvk(jline,key,entity):
    ret=[]
    data=getmarc(jline,key,None)
    if isinstance(data,list):
        for elem in data:
            ret.append(handle_single_rvk(elem))
        return ret
    elif isinstance(data,dict):
        return handle_single_rvk(data)
            
def handle_single_rvk(data):
    sset={}
    record={}
    if not "rv" in data:
        return
    for subfield in data.get("rv"):
        for k,v in subfield.items():
            sset[k]=v
    if "0" in sset:
        record["sameAs"]=gnd2uri("(DE-576)"+sset.get("0"))
    if "a" in sset:
        record["@id"]="https://rvk.uni-regensburg.de/api/json/ancestors/"+sset.get("a").replace(" ","+")
        record["identifier"]={  "@type"     :"PropertyValue",
                                "propertyID":"RVK",
                                "value"     :sset.get("a")}
    if "b" in sset:
        record["name"]=sset.get("b")
    if "k" in sset:
        record["keywords"]=sset.get("k")
    return record
    
def handlePPN(jline,key,entity):
    return {    "@type"     :"PropertyValue",
                "propertyID":"Pica Product Number",
                "value"     :jline.get(key)}

def relatedTo(jline,key,entity):
    #e.g. split "551^4:orta" to 551 and orta
    marcfield=key[:3]
    data=[]
    if marcfield in jline:
        for array in jline[marcfield]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                if isinstance(sset.get("9"),str) and sset.get("9") in marc2relation:
                    node={}
                    node["_key"]=marc2relation[sset.get("9")]
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith("http"):
                            node["sameAs"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,str):
                            node["identifier"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem.startswith("http"):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
                elif isinstance(sset.get("9"),list):
                    node={}
                    for elem in sset.get("9"):
                        if elem.startswith("v"):
                            for k,v in marc2relation.items():
                                if k.lower() in elem.lower():
                                    node["_key"]=v
                                    break
                        elif [x for x in marc2relation if x.lower() in elem.lower()]:
                            for x in marc2relation:
                                if x.lower() in elem.lower():
                                    node["_key"]=marc2relation[x]
                        elif not node.get("_key"):
                            node["_key"]="relatedTo"
                        #eprint(elem,node)
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith("http"):
                            node["sameAs"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,str):
                            node["identifier"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem.startswith("http"):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
                    #eprint(node)
                    
        if data:
            return ArrayOrSingleValue(data)
        
def get_subfield_if_4(jline,key,entity):
    #e.g. split "551^4:orta" to 551 and orta
    marcfield=key.rsplit("^")[0]
    subfield4=key.rsplit("^")[1]
    data=[]
    if marcfield in jline:
        for array in jline[marcfield]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                if "9" in sset and subfield4 in sset.get("9"):
                    node={}
                    if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith("http"):
                            node["sameAs"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,str):
                            node["identifier"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem.startswith("http"):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                    if sset.get("a"):
                        node["name"]=sset.get("a")
                    data.append(node)
        if data:
            return ArrayOrSingleValue(data)

def get_subfield(jline,key,entity):
    #e.g. split "551^4:orta" to 551 and orta
    data=[]
    if key in jline:
        for array in jline[key]:
            for k,v in array.items():
                sset={}
                for subfield in v:
                    for subfield_code in subfield:
                        sset[subfield_code]=subfield[subfield_code]
                #eprint(sset.get("9"),subfield4)
                node={}
                if sset.get("0"):
                        uri=gnd2uri(sset.get("0"))
                        if isinstance(uri,str) and uri.startswith("http"):
                            node["sameAs"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,str):
                            node["identifier"]=gnd2uri(sset.get("0"))
                        elif isinstance(uri,list):
                            node["sameAs"]=None
                            node["identifier"]=None
                            for elem in uri:
                                if elem and elem.startswith("http"):
                                    node["sameAs"]=litter(node["sameAs"],elem)
                                else:
                                    node["identifier"]=litter(node["identifier"],elem)
                if sset.get("a"):
                    node["name"]=sset.get("a")
                for typ in ["D","d"]:
                    if sset.get(typ):   #http://www.dnb.de/SharedDocs/Downloads/DE/DNB/wir/marc21VereinbarungDatentauschTeil1.pdf?__blob=publicationFile Seite 14
                        node["@type"]="http://schema.org/"
                        if sset.get(typ)=="p":
                            node["@type"]+="Person"
                        elif sset.get(typ)=="b":
                            node["@type"]+="Organization"
                        elif sset.get(typ)=="f":
                            node["@type"]+="Event"
                        elif sset.get(typ)=="u":
                            node["@type"]+="CreativeWork"
                        elif sset.get(typ)=="g":
                            node["@type"]+="Place"
                        else:
                            node.pop("@type")
                            
                if node:
                    data.append(node)
        if data:
            return  ArrayOrSingleValue(data)

def deathDate(jline,key,entity):
    return marc_dates(jline.get(key),"deathDate")

def birthDate(jline,key,entity):
    return marc_dates(jline.get(key),"birthDate")

def marc_dates(record,event):
    data=None
    subset=None 
    recset={}
    if record:
        for indicator_level in record:
            for subfield in indicator_level:
                sset={}
                for sf_elem in indicator_level.get(subfield):
                    for k,v in sf_elem.items():
                        if k=="a" or k=="9":
                            sset[k]=v
                if isinstance(sset.get("9"),str):
                    recset[sset['9']]=sset.get("a")
                elif isinstance(sset.get("9"),list):
                    for elem in sset.get("9"):
                        if elem.startswith("4:dat"):
                            recset[elem]=sset.get("a")
    #eprint(recset)
    if recset.get("4:datx"):
        return dateToEvent(recset["4:datx"],event)
    elif recset.get("4:datl"):
        return dateToEvent(recset["4:datl"],event)
    else:
        return None

def getparent(jline,key,entity):
    data=None
    try:
        for i in jline[key[0][0:3]][0]:
            sset={}
            for j in jline[key[0][0:3]][0][i]:
                for k,v in dict(j).items():
                    sset[k]=v
                conti=False
                if "9" in sset:
                    if sset["9"]=='4:adue':
                            conti=True
                if conti and "0" in sset:
                    data=litter(data,gnd2uri(sset["0"]))
    except Exception as e:
        with open("errors.txt",'a') as f:
            traceback.print_exc(file=f)
    if data:
        return data

def honorificSuffix(jline,key,entity):
    data=None
    if key[0][:3] in jline:
        for i in jline.get(key[0][0:3])[0]:
            sset={}
            for j in jline.get(key[0][0:3])[0][i]:
                for k,v in dict(j).items():
                    sset[k]=v
                conti=False
                if "9" in sset:
                    if sset["9"]=='4:adel' or sset["9"]=='4:akad':
                            conti=True
                if conti and "a" in sset:
                    data=litter(data,sset["a"])
    if data:
        return data

    
def hasOccupation(jline,key,entity):
    data=[]
    if key[:3] in jline:
        for i in jline[key[:3]]:
            conti=False
            job=None
            sset={}
            for k,v in i.items():               # v = [{'0': ['(DE-576)210258373', '(DE-588)4219681-4']}, {'a': 'Romanist'}, {'9': '4:berc'}, {'w': 'r'}, {'i': 'Charakteristischer Beruf'}]
                for w in v:                     # w = {'0': ['(DE-576)210258373', '(DE-588)4219681-4']}
                    for c,y in dict(w).items(): # c =0 y = ['(DE-576)210258373', '(DE-588)4219681-4']
                        sset[c]=y
            if "9" in sset:                     #sset = {'a': 'Romanist', 'w': 'r', '0': ['(DE-576)210258373', '(DE-588)4219681-4'], 'i': 'Charakteristischer Beruf', '9': '4:berc'}
                if sset["9"]=='4:berc' or sset["9"]=='4:beru' or sset['9']=='4:akti':
                        conti=True
            if conti:
                if key[-1] in sset:
                    if isinstance(sset[key[-1]],list):
                        for field in sset[key[-1]]:
                            if field[1:7] in isil2sameAs:
                                data.append(gnd2uri(field))
    if data:
        return ArrayOrSingleValue(data)



def getgeo(arr):
    for k,v in traverse(arr,""):
        if isinstance(v,str):
            if '.' in v:
                return v


            #key : {"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}
def getGeoCoordinates(record,key,entity):
    ret={}
    for k,v in key.items():
        coord=getgeo(getmarc(record,v,entity))
        if coord:
            ret[k]=coord
    if ret:
        return ret

def removeNone(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(removeNone(x) for x in obj if x is not None)
    elif isinstance(obj, dict):
        return type(obj)((removeNone(k), removeNone(v))
            for k, v in obj.items() if k is not None and v is not None)
    else:
        return obj

def removeEmpty(obj):
    if isinstance(obj,dict):
        toDelete=[]
        for k,v in obj.items():
            if v:
                v = ArrayOrSingleValue(removeEmpty(v))
            else:
                toDelete.append(k)
        for key in toDelete:
            obj.pop(key)
        return obj
    elif isinstance(obj,str):
        return obj
    elif isinstance(obj,list):
        for elem in obj:
            if elem:
                elem = removeEmpty(elem)
            else:
                del elem
        return obj


#make data more RDF
def check(ldj,entity):
    ldj=removeNone(removeEmpty(ldj))
    for k,v in ldj.items():
        v=ArrayOrSingleValue(v)
    if not ldj:
        return
    if 'genre' in ldj:
        genre=ldj.pop('genre')
        ldj['genre']={}
        ldj['genre']['@type']="Text"
        ldj['genre']["Text"]=genre
    if ldj.get("identifier") and ldj.get("_isil") and isil2sameAs.get(ldj.get("_isil")):
        if "sameAs" in ldj and isinstance(ldj.get("sameAs"),str):
            ldj["sameAs"]=[ldj.pop("sameAs")]
            ldj["sameAs"].append(uri2url(ldj["_isil"],ldj.pop("identifier")))
        elif "sameAs" in ldj and isinstance(ldj.get("sameAs"),list):
            ldj["sameAs"].append(uri2url(ldj["_isil"],ldj.pop("identifier")))
        else:
            ldj["sameAs"]=uri2url(ldj.get("_isil"),ldj.get("identifier"))
    ldj["identifier"]=ldj.get("@id").split("/")[4]
    if 'numberOfPages' in ldj:
        numstring=ldj.pop('numberOfPages')
        try:
            if isinstance(numstring,str):
                if "S." in numstring and isint(numstring.split('S.')[0].strip()):
                    num=int(numstring.split('S.')[0].strip())
                    ldj['numberOfPages']=num
            elif isinstance(numstring,list):
                for number in numstring:
                    if "S." in number and isint(number.split('S.')[0].strip()):
                        num=int(number.split('S.')[0])
                        ldj['numberOfPages']=num
        except IndexError:
            if isint(numstring):
                ldj['numberOfPages']=numstring
            else:
                pass
        except Exception as e:
            with open("error.txt","a") as err:
                print(e,file=err)
    #if entity=="Person":
        #checks=["relatedTo","hasOccupation","birthPlace","deathPlace"]
        #for key in checks:
            #if key in ldj:
                #if isinstance(ldj[key],list):
                    #for pers in ldj[key]:
                        #if "@id" not in pers:
                            #del pers
                #elif isinstance(ldj[key],dict):
                    #if "@id" not in ldj[key]:
                        #ldj.pop(key)
                #elif isinstance(ldj[key],str):
                    #ldj.pop(key)
    for label in ["name","alternativeHeadline","alternateName"]:
        try:
            if label in ldj:
                if isinstance(ldj[label],str):
                    if ldj[label][-2:]==" /":
                        ldj[label]=ldj[label][:-2]
                elif isinstance(ldj[label],list):
                    for n,i in enumerate(ldj[label]):
                        if i[-2:]==" /":
                            ldj[label][n]=i[:-2]
                    if label=="name":
                        name=" ".join(ldj[label])
                        ldj[label]=name
        except:
            eprint(ldj,label)
    if "publisherImprint" in ldj:
        if not isinstance(ldj["@context"],list) and isinstance(ldj["@context"],str):
            ldj["@context"]=list([ldj.pop("@context")])
        ldj["@context"].append(URIRef(u'http://bib.schema.org/'))
    if "isbn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/Book')
    if "issn" in ldj:
        ldj["@type"]=URIRef(u'http://schema.org/CreativeWorkSeries')
    if "pub_name" in ldj or "pub_place" in ldj:
        ldj["publisher"]={}
        if "pub_name" in ldj:
            ldj["publisher"]["name"]=ldj.pop("pub_name")
        if "pub_place" in ldj:
            ldj["publisher"]["location"]=ldj.pop("pub_place")
        for value in ["name","location"]:        # LOD-JIRA Ticket #105
            if ldj.get("publisher") and ldj.get("publisher").get(value) and isinstance(ldj.get("publisher").get(value),str)and ldj.get("publisher").get(value)[-1] in [",",":",";"]:
                ldj["publisher"][value]=ldj.get("publisher").get(value)[:-1].strip()
    if "sameAs" in ldj:
        ldj["sameAs"]=removeNone(cleanup_sameAs(ldj.pop("sameAs")))
            
    return ldj

def cleanup_sameAs(sameAs):
    if isinstance(sameAs,list):
        for n,elem in enumerate(sameAs):
            sameAs[n]=cleanup_sameAs(elem)
    elif isinstance(sameAs,str):
        if not sameAs.strip().startswith("http"):
            return None
    return sameAs
        

map_entities={
        "p":"persons",      #Personen, individualisiert
        "n":"persons",      #Personen, namen, nicht individualisiert
        "s":"tags",        #Schlagwörter/Berufe
        "b":"orga",         #Organisationen
        "g":"geo",          #Geographika
        "u":"works",     #Werktiteldaten
}

def getentity(record):
    zerosevenninedotb=getmarc(record,"079..b",None)
    if zerosevenninedotb in map_entities:
        return map_entities[zerosevenninedotb]
    elif not zerosevenninedotb:
        return "resources"  # Titeldaten ;)
    else:
        return

entities = {
   "resources":{   # mapping is 1:1 like works
        "@type"             :"http://schema.org/CreativeWork",
        "@context"      :"http://schema.org",
        "@id"           :get_or_generate_id,
        "identifier"    :{getmarc:"001"},
        "_isil"         :{getmarc:["003","852..a"]},
        "dateModified"   :{getmarc:"005"},
        "sameAs"        :{getmarc:["024..a","670..u"]},
        "name"              :{getmarc:["130..a","130..p","245..a","245..b"]},
        "disambiguatingDescription"       :{getmarc:["245..c"]},
        "alternateName"     :{getmarc:["240..a","240..p","246..a","246..b","245..p","249..a","249..b","730..a","730..p","740..a","740..p","920..t"]},
        "author"            :{get_subfield:"100"},
        "contributor"       :{get_subfield:"700"},
        "pub_name"          :{getmarc:["260..b","264..b"]},
        "pub_place"         :{getmarc:["260..a","264..a"]},
        "datePublished"     :{getmarc:["130..f","260..c","264..c","362..a"]},
        "Thesis"            :{getmarc:["502..a","502..b","502..c","502..d"]},
        "issn"              :{getmarc:["022..a","022..y","022..z","029..a","490..x","730..x","773..x","776..x","780..x","785..x","800..x","810..x","811..x","830..x"]},
        "isbn"              :{getmarc:["022..a","022..z","776..z","780..z","785..z"]},
        "genre"             :{getmarc:"655..a"},
        "hasPart"           :{getmarc:"773..g"},
        "isPartOf"          :{getmarc:["773..t","773..s","773..a"]},
        "license"           :{getmarc:"540..a"},
        "inLanguage"        :{getmarc:["377..a","041..a","041..d","130..l","730..l"]},
        "numberOfPages"     :{getmarc:["300..a","300..b","300..c","300..d","300..e","300..f","300..g"]},
        "pageStart"         :{getmarc:"773..q"},
        "issueNumber"       :{getmarc:"773..l"},
        "volumeNumer"       :{getmarc:"773..v"},
        "locationCreated"   :{get_subfield_if_4:"551^4:orth"},
        "relatedTo"         :{relatedTo:"500..0"},
        "about"             :{handle_rvk:"936"},
        "description"       :{getmarc:"520..a"},
        "mentions"          :{get_subfield:"689"}
        },
    "works":{   # mapping is 1:1 like resources
        "@type"             :"http://schema.org/CreativeWork",
        "@context"      :"http://schema.org",
        "@id"           :get_or_generate_id,
        "identifier"    :{getmarc:"001"},
        "_isil"         :{getmarc:"003"},
        "dateModified"   :{getmarc:"005"},
        "sameAs"        :{getmarc:["024..a","670..u"]},
        "name"              :{getmarc:["130..a","130..p","245..a","245..b"]},
        "disambiguatingDescription"      :{getmarc:["245..c"]},
        "alternateName"     :{getmarc:["240..a","240..p","246..a","246..b","245..p","249..a","249..b","730..a","730..p","740..a","740..p","920..t"]},
        "author"            :{get_subfield:"100"},
        "contributor"       :{get_subfield:"700"},
        "pub_name"          :{getmarc:["260..b","264..b"]},
        "pub_place"         :{getmarc:["260..a","264..a"]},
        "datePublished"     :{getmarc:["130..f","260..c","264..c","362..a"]},
        "Thesis"            :{getmarc:["502..a","502..b","502..c","502..d"]},
        "issn"              :{getmarc:["022..a","022..y","022..z","029..a","490..x","730..x","773..x","776..x","780..x","785..x","800..x","810..x","811..x","830..x"]},
        "isbn"              :{getmarc:["022..a","022..z","776..z","780..z","785..z"]},
        "genre"             :{getmarc:"655..a"},
        "hasPart"           :{getmarc:"773..g"},
        "isPartOf"          :{getmarc:["773..t","773..s","773..a"]},
        "license"           :{getmarc:"540..a"},
        "inLanguage"        :{getmarc:["377..a","041..a","041..d","130..l","730..l"]},
        "numberOfPages"     :{getmarc:["300..a","300..b","300..c","300..d","300..e","300..f","300..g"]},
        "pageStart"         :{getmarc:"773..q"},
        "issueNumber"       :{getmarc:"773..l"},
        "volumeNumer"       :{getmarc:"773..v"},
        "locationCreated"   :{get_subfield_if_4:"551^4:orth"},
        "relatedTo"         :{relatedTo:"500..0"}
        },
    "persons": {
        "@type"         :"http://schema.org/Person",
        "@context"      :"http://schema.org",
        "@id"           :get_or_generate_id,
        "identifier"    :{getmarc:"001"},
        "_isil"         :{getmarc:"003"},
        "dateModified"   :{getmarc:"005"},
        "sameAs"        :{getmarc:["024..a","670..u"]},
        
        "name"          :{getmarc:"100..a"},
        "gender"        :{handlesex:"375..a"},
        "alternateName" :{getmarc:["400..a","400..c"]},
        "relatedTo"     :{relatedTo:"500..0"},
        "hasOccupation" :{hasOccupation:"550..0"},
        "birthPlace"    :{get_subfield_if_4:"551^4:ortg"},
        "deathPlace"    :{get_subfield_if_4:"551^4:orts"},
        "honorificSuffix" :{honorificSuffix:["550..0","550..i","550..a","550..9"]},
        "birthDate"     :{birthDate:"548"},
        "deathDate"     :{deathDate:"548"},
        "workLocation"  :{get_subfield_if_4:"551^4:ortw"},
    },
    "orga": {
        "@type"             :"http://schema.org/Organization",
        "@context"          :"http://schema.org",
        "@id"               :get_or_generate_id,
        "identifier"        :{getmarc:"001"},
        "_isil"             :{getmarc:"003"},
        "dateModified"       :{getmarc:"005"},
        "sameAs"            :{getmarc:["024..a","670..u"]},
        
        "name"              :{getmarc:"110..a"},
        "alternateName"     :{getmarc:"410..a+b"},
        
        "additionalType"    :{get_subfield_if_4:"550^4:obin"},
        "parentOrganization":{get_subfield_if_4:"551^4:adue"},
        "location"          :{get_subfield_if_4:"551^4:orta"},
        "fromLocation"      :{get_subfield_if_4:"551^4:geoa"},
        "areaServed"        :{get_subfield_if_4:"551^4:geow"},
        },
    "geo": {
        "@type"             :"http://schema.org/Place",
        "@context"          :"http://schema.org",
        "@id"               :get_or_generate_id,
        "identifier"        :{getmarc:"001"},
        "_isil"             :{getmarc:"003"},
        "dateModified"       :{getmarc:"005"},
        "sameAs"            :{getmarc:["024..a","670..u"]},
        
        "name"              :{getmarc:"151..a"},
        "alternateName"     :{getmarc:"451..a"},
        "description"       :{get_subfield:"551"},
        "GeoCoordinates"    :{getGeoCoordinates:{"longitude":["034..d","034..e"],"latitude":["034..f","034..g"]}},
        },
    "tags":{                   #generisches Mapping für Schlagwörter
        "@type"             :"http://schema.org/Thing",
        "@context"          :"http://schema.org",
        "@id"               :get_or_generate_id,
        "identifier"        :{getmarc:"001"},
        "_isil"             :{getmarc:"003"},
        "dateModified"       :{getmarc:"005"},
        "sameAs"            :{getmarc:["024..a","670..u"]},
        
        "name"              :{getmarc:"150..a"},
        "alternateName"     :{getmarc:"450..a+x"},
        "description"       :{getmarc:"679..a"},
        "additionalType"    :{get_subfield:"550"},
        "location"          :{get_subfield_if_4:"551^4:orta"},
        "fromLocation"      :{get_subfield_if_4:"551^4:geoa"},
        "areaServed"        :{get_subfield_if_4:"551^4:geow"},
        "contentLocation"   :{get_subfield_if_4:"551^4:punk"},
        "participant"       :{get_subfield_if_4:"551^4:bete"},
        "relatedTo"         :{get_subfield_if_4:"551^4:vbal"},
        }
}

def traverse(dict_or_list, path):
    iterator=None
    if isinstance(dict_or_list, dict):
        iterator = dict_or_list.items()
    elif isinstance(dict_or_list, list):
        iterator = enumerate(dict_or_list)
    elif isinstance(dict_or_list,str):
        strarr=[]
        strarr.append(dict_or_list)
        iterator=enumerate(strarr)
    else:
        return
    if iterator:
        for k, v in iterator:
            yield path + str([k]), v
            if isinstance(v, (dict, list)):
                for k, v in traverse(v, path + str([k])):
                    yield k, v


def get_source_include_str():
    items=["079"]
    for k,v in traverse(entities,""):
        if isinstance(v,str) and isint(v[:3]) and v not in items:
                items.append(v[:3])
    _source=",".join(items)
    return _source
    
def process_field(record,value,entity):
    ret=[]
    if isinstance(value,dict):
        for function,parameter in value.items():
            ret.append(function(record,parameter,entity))
    elif isinstance(value,str):
        return value
    elif isinstance(value,list):
        for elem in value:
            ret.append(ArrayOrSingleValue(process_field(record,elem,entity)))
    elif callable(value):
        return ArrayOrSingleValue(value(record,entity))
    if ret:
        return ArrayOrSingleValue(ret)


#processing a single line of json without whitespace 
def process_line(jline,host,port,index,type):
    entity=getentity(jline)
    if entity:
        mapline={}
        mapline["@type"]=[]
        mapline["@type"].append(URIRef(u'http://schema.org/'+entity))
        mapline["@context"]=[URIRef(u'http://schema.org')]
        for key,val in entities[entity].items():
            value=process_field(jline,val,entity)
            if value:
                if "related" in key:
                    if isinstance(value,dict) and "_key" in value:
                        dictkey=value.pop("_key")
                        mapline[dictkey]=value
                    elif isinstance(value,list):
                        for elem in value:
                            if isinstance(elem,dict) and "_key" in elem:
                                relation=elem.pop("_key")
                                dictkey=relation
                                if dictkey not in mapline:
                                    mapline[dictkey]=[elem]
                                elif isinstance(mapline[dictkey],list):
                                    mapline[dictkey].append(elem)
                                elif isinstance(mapline[dictkey],dict):
                                    old=mapline.pop(dictkey)
                                    mapline[dictkey]=[elem]
                                    mapline[dictkey]=litter(mapline[dictkey],elem)
                            elif isinstance(elem,dict):
                                if key not in mapline:
                                    mapline[key]=[elem]
                                else:
                                    mapline[key].append(elem)
                else:
                    mapline[key]=value
        if mapline:
            mapline=check(mapline,entity)
            if host and port and index and type:
                mapline["url"]="http://"+host+":"+str(port)+"/"+index+"/"+type+"/"+getmarc(jline,"001",None)+"?pretty"
            return {entity:mapline}
    
def output(entity,mapline,outstream):
    if outstream:
        outstream[entity].write(json.dumps(mapline,indent=None)+"\n")
    else:
        sys.stdout.write(json.dumps(mapline,indent=None)+"\n")
        sys.stdout.flush()


def setupoutput(prefix):
    if prefix:
        if not os.path.isdir(prefix):
            os.mkdir(prefix)
        if not prefix[-1]=="/":
            prefix+="/"
    else:
        prefix=""
    for entity in entities:
        if not os.path.isdir(prefix+entity):
            os.mkdir(prefix+entity)

def init_mp(h,p,pr):
    global host
    global port
    global outstream
    global filepaths
    global prefix
    if not pr:
        prefix = ""
    elif pr[-1]!="/":
        prefix=pr+"/"
    else:
        prefix=pr
    port = p
    host = h
    outstream={}
    filepaths=[]

def worker(ldj):
    out={}
    for entity in entities:
            out[entity]=open(prefix+entity+"/"+str(current_process().name)+"-records.ldj","a")
    for source_record in ldj:
        record=source_record.pop("_source")
        type=source_record.pop("_type")
        index=source_record.pop("_index")
        target_record=process_line(record,host,port,index,type)
        if target_record:
            for entity in target_record:
                out[entity].write(json.dumps(target_record[entity],indent=None)+"\n")
    for entity in entities:
        out[entity].close()
        
        
        
        
if __name__ == "__main__":
    #argstuff
    parser=argparse.ArgumentParser(description='Entitysplitting/Recognition of MARC-Records')
    parser.add_argument('-host',type=str,help='hostname or IP-Address of the ElasticSearch-node to use. If None we try to read ldj from stdin.')
    parser.add_argument('-port',type=int,default=9200,help='Port of the ElasticSearch-node to use, default is 9200.')
    parser.add_argument('-type',type=str,help='ElasticSearch Type to use')
    parser.add_argument('-index',type=str,help='ElasticSearch Index to use')
    parser.add_argument('-id',type=str,help='map single document, given by id')
    parser.add_argument('-help',action="store_true",help="print this help")
    parser.add_argument('-prefix',type=str,default="ldj/",help='Prefix to use for output data')
    parser.add_argument('-debug',action="store_true",help='Dump processed Records to stdout (mostly used for debug-purposes)')
    parser.add_argument('-server',type=str,help="use http://host:port/index/type/id?pretty syntax. overwrites host/port/index/id/pretty")
    parser.add_argument('-pretty',action="store_true",default=False,help="output tabbed json")
    parser.add_argument('-w',type=int,default=8,help="how many processes to use")
    args=parser.parse_args()
    if args.server:
        slashsplit=args.server.split("/")
        args.host=slashsplit[2].rsplit(":")[0]
        if isint(args.server.split(":")[2].rsplit("/")[0]):
            args.port=args.server.split(":")[2].split("/")[0]
        args.index=args.server.split("/")[3]
        if len(slashsplit)>4:
            args.type=slashsplit[4]
        if len(slashsplit)>5:
            if "?pretty" in args.server:
                args.pretty=True
                args.id=slashsplit[5].rsplit("?")[0]
            else:
                args.id=slashsplit[5]
    if args.pretty:
        tabbing=4
    else:
        tabbing=None
    if args.help:
        parser.print_help(sys.stderr)
        exit()        
    elif args.host and args.index and args.type and args.id:
        es=elasticsearch.Elasticsearch([{"host":args.host}],port=args.port)
        json_record=None
        source=get_source_include_str()
        json_record=es.get_source(index=args.index,doc_type=args.type,id=args.id,_source=source)
        if json_record:
            print(json.dumps(process_line(json_record,args.host,args.port,args.index,args.type),indent=tabbing))
    elif args.host and args.index and args.type and args.debug:
        init_mp(args.host,args.port,None)
        for ldj in esgenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       source=get_source_include_str(),
                       headless=True
                        ): 
            record = process_line(ldj,args.host,args.port,args.index,args.type)
            if record:
                for k in record:
                    print(json.dumps(record[k],indent=None))
    elif args.host and args.index and args.type : #if inf not set, than try elasticsearch
        setupoutput(args.prefix)
        pool = Pool(args.w,initializer=init_mp,initargs=(args.host,args.port,args.prefix))
        for ldj in esfatgenerator(host=args.host,
                       port=args.port,
                       index=args.index,
                       type=args.type,
                       source=get_source_include_str()
                        ):
            pool.apply_async(worker,args=(ldj,))
        pool.close()
        pool.join()

        quit(0)
    else: #oh noes, no elasticsearch input-setup. then we'll use stdin
        eprint("No host/port/index specified, trying stdin\n")
        init_mp("localhost","DEBUG","DEBUG")
        with io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8') as input_stream:
            for line in input_stream:
                ret=process_line(json.loads(line),"localhost",9200,"data","mrc")
                if isinstance(ret,dict):
                    for k,v in ret.items():
                        print(json.dumps(v,indent=tabbing))
