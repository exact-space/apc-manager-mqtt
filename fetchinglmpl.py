import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import json
import requests
import grequests
import os
import datetime
import time
import statistics
import math
import sys
import itertools
import traceback

import platform
version = platform.python_version().split(".")[0]
if version == "3":
    import app_config.app_config as cfg
elif version == "2":
    import app_config as cfg
config = cfg.getconfig()


tagType = "apcManager"


class fetching():
    def __init__(self,unitsIdList):
        self.unitsIdList = unitsIdList


    def getResponseBody(self,response,word="",printa=False):
        try:
            if(response.status_code==200):
                if printa:
                    print(f"Got {word} successfully.....")

                body = json.loads(response.content)
                if type(body) != list:
                    body = [body]
                
            else:
                body =[]
                print(f"Did not get{word} successfully.....")
                print(response.status_code)
                print(response.content)
            return body
        except:
            print(traceback.format_exc())


    # --------------------------- apc meta related start -------------------- #
    def getTagMeta(self,query,retrunAsList = False):
        try:
            urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'
            # print(urlQuery)
            response = requests.get(urlQuery)
            word = "tagmeta"
            body = self.getResponseBody(response,word,1)
            if retrunAsList:
                returnLst = []
                for i in body:
                    returnLst.append(i["dataTagId"])
                return returnLst
            else:
                return body
        except:
            print(traceback.format_exc())


    def getPrefixFromUnitsId(self,unitsId):
        
        url = config["api"]["meta"]+"/ingestconfigs"
        ingestdf = pd.DataFrame(requests.get(url).json())
        ingestdf = ingestdf[(ingestdf["unitsId"]==unitsId) &(ingestdf["PROG_ID_PREFER"]==1.0) ]
        prefix = ingestdf.loc[ingestdf.index[0],"TAG_PREFIX"]
        return prefix


    def getUnitName(self,unitsId):
        try:
            query = {
                "id":unitsId
            }

            urlQuery = config["api"]["meta"] + '/units?filter={"where":' + json.dumps(query) + '}'  
            response = requests.get(urlQuery)
            if(response.status_code==200):
                # print(response.status_code)
                # print("Got Calculations  successfully.....")
                body = json.loads(response.content)[0]
                return body["name"]
                
            else:
                body =[]
                print("Did not get unit name successfully.....")
                print(response.status_code)
                print(response.content)
            return body
        except:
            print(traceback.format_exc())


    def getCalculationsFromDataTagId(self,dataTagId):
        try:
            query = {
                "dataTagId":dataTagId
            }

            urlQuery = config["api"]["meta"] + '/calculations?filter={"where":' + json.dumps(query) + '}'  
            print(urlQuery)
            response = requests.get(urlQuery)
            if(response.status_code==200):
                # print(response.status_code)
                # print("Got Calculations  successfully.....")
                body = json.loads(response.content)
                
            else:
                body =[]
                print("Did not get calculations successfully.....")
                print(response.status_code)
                print(response.content)
            return body
        except:
            print(traceback.format_exc())


    def getTagmetaFromDataTagId(self,dataTagId):
        try:
            query = {
                "dataTagId":dataTagId
            }

            urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'  
            response = requests.get(urlQuery)
            if(response.status_code==200):
                # print(response.status_code)
                # print("Got tagmeta successfully.....")
                body = json.loads(response.content)
                
            else:
                body =[]
                print("Did not get tagmeta successfully.....")
                print(response.status_code)
                print(response.content)
            return body
        except:
            print(traceback.format_exc())

    
    def getTagmetaFromUnitsId(self,unitsIdList,field = False):
        try:
            urls = []
            for unitsId in unitsIdList:
                
                query ={
                    "unitsId":unitsId,
                    "measureProperty":"Power",
                    "measureType":"Apc",
                    "tagType" : "apcManager"
                    
                }
                if not field:
                    urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'  
                else:
                    fields = ["dataTagId","description","unitsId"]
                    urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + ',"fields":'+ json.dumps(fields) +'}'  
 

                urls.append(urlQuery)
                # print(urlQuery)
            

            rs = (grequests.get(u) for u in urls)
            requests = grequests.map(rs)
            
            tagmeta = {}
            for idx,response in enumerate(requests):
                if response.status_code==200:
                    # print("got tagmeta successfully...")
                    tagmeta[unitsIdList[idx]] = json.loads(response.content)
                else:
                    print("Not getting tagmeta SL Level successfully...")
                    print(response.status_code)
                    print(response.content)
            # print(tagmeta)
            return tagmeta
    

        except Exception as e:
            print(traceback.format_exc())


    def getTagmetaForSL(self,unitsIdList):
        try:
            urls=[]
            for unitsId in unitsIdList:
                query =  {
                    "unitsId":unitsId,
                    "measureProperty":"Equipment Apc",
                    "or":[
                    {"measureType":"Product"},
                    {"measureType":"Ratio"},
                   
                    ]
                }
                urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'  
                urls.append(urlQuery)
                # print(urlQuery)
            

            rs = (grequests.get(u) for u in urls)
            requests = grequests.map(rs)
            
            tagmeta = {}
            for idx,response in enumerate(requests):
                if response.status_code==200:
                    # print("got tagmeta successfully...")
                    tagmeta[unitsIdList[idx]] = json.loads(response.content)
                else:
                    print("Not getting tagmeta successfully...")
                    print(response.status_code)
                    print(response.content)
            # print(tagmeta)
            return tagmeta
        except:
            print(traceback.format_exc())


    def getTagmetaForUL(self,unitsIdList):
        try:
            urls=[]
            for unitsId in unitsIdList:
                query =  {
                    "unitsId":unitsId,
                    "measureProperty":"System Apc",
                    "equipment":"Performance Kpi",
                    "equipmentName":"Performance Kpi",
                }
                urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'  
                urls.append(urlQuery)
                # print(urlQuery)
            

            rs = (grequests.get(u) for u in urls)
            requests = grequests.map(rs)
            
            tagmeta = {}
            for idx,response in enumerate(requests):
                if response.status_code==200:
                    # print("got tagmeta successfully...")
                    tagmeta[unitsIdList[idx]] = json.loads(response.content)
                else:
                    print("Not getting tagmeta successfully...")
                    print(response.status_code)
                    print(response.content)
            # print(tagmeta)
            return tagmeta
        except:
            print(traceback.format_exc())


    def getTagmetaForDel(self):
        try:
            urls = []
            for unitsId in self.unitsIdList:
                
                query ={
                    "unitsId":unitsId,
                    # "or":[
                    # {"measureProperty":"Equipment Apc"},
                    # {"measureProperty":"System Apc"},
                    # {"measureProperty":"Unit Apc"},
                    # {"measureProperty":"ratio"},
                    # ],
                    "tagType" : "apcManager"

                }
                urlQuery = config["api"]["meta"] + '/tagmeta?filter={"where":' + json.dumps(query) + '}'  
                urls.append(urlQuery)
                print(urlQuery)
            
            
            
            rs = (grequests.get(u) for u in urls)
            requests = grequests.map(rs)
            
            tagmeta = []
            for idx,response in enumerate(requests):
                if response.status_code==200:
                    # print("got tagmeta successfully...")
                    tagmeta += json.loads(response.content)
                else:
                    print("Not getting tagmeta SL Level successfully...")
                    print(response.status_code)
                    print(response.content)
            # print(tagmeta)
            return tagmeta
        
        except:
            print(traceback.format_exc())


    def getCalForDel(self,dataTagIdLst):
        try:
            urls = []
            for dataTagId in dataTagIdLst:
                
                query ={
                    "dataTagId":dataTagId
                }

                urlQuery = config["api"]["meta"] + '/calculations?filter={"where":' + json.dumps(query) + '}'  
                urls.append(urlQuery)
                # print(urlQuery)
            

            rs = (grequests.get(u) for u in urls)
            requests = grequests.map(rs)
            
            tagmeta = []
            for idx,response in enumerate(requests):
                if response.status_code==200:
                    # print("got tagmeta successfully...")
                    tagmeta += json.loads(response.content)

                else:
                    
                    print("Not getting tagmeta SL Level successfully...")
                    print(response.status_code)
                    print(response.content)
            # print(tagmeta)
            return tagmeta
        
        except:
            print(traceback.format_exc())
    # --------------------------- apc meta related end -------------------- #


    def historicDataReq(self,body):
        try:
            unitsId = body["unitsId"]
            id = body["id"]
            print("current datatag:",body["dataTagId"])
            url = config["api"]["meta"].replace("exactapi","") + f"service/launch/{unitsId}/historic-calculations?CALCULATION_ID={id}"
            print(url)
            res = requests.get(url)
            statscode = res.status_code
            print("status code", statscode)
            if statscode != 200:
                print("historic cal req failed")
                print(res.status_code)
                print(res.content)
        except:
            print(traceback.format_exc())