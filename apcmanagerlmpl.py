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
from fetchinglmpl import fetching


import platform
version = platform.python_version().split(".")[0]
if version == "3":
    import app_config.app_config as cfg
elif version == "2":
    import app_config as cfg
config = cfg.getconfig()

loadQuery = {
                    "measureInstance" : 1,
                    "equipment" : "Generator",
                    "system" : "Generator System",
                    "measureType" : "Load",
                    "measureProperty" : "Power"
                }

msfQuery = {
    "measureInstance" : 1,
    "equipment" : "Final Superheater",
    "measureType" : "Flow",
    "measureProperty" : "Main Steam"
}

def tr():
    print(traceback.format_exc())

def pp(s):
    print(json.dumps(s,indent=4))


class posting(fetching):
    def postInTagmeta(self,postBody):
        sumTagName = postBody["dataTagId"]
        unitsId = postBody["unitsId"]

        checkBody = self.getTagmetaFromDataTagId(sumTagName)

        if len(checkBody)>1:
            print("ALEART!!!!!!!!!!!!")
            print(f"Found multiple traces of {sumTagName} in tagmeta")

        if len(checkBody) == 0:
            del postBody["id"]
            url = config["api"]["meta"] + f"/units/{unitsId}/tagmeta"
            # pp(postBody)
            response = requests.post(url,json=postBody)

            if response.status_code == 200 or response.status_code == 204:
                print(f"{sumTagName} Tagmeta posting successful..")

            else:
                print(f"{sumTagName} Tagmeta posting unsuccessful..")
                print(response.status_code,response.content)

        else:
            # print(len(checkBody))
            # print(f"{sumTagName} is already in tagmeta so updating...")
            postBody["id"] = checkBody[0]["id"]
            self.updateTagmeta(postBody,checkBody[0]["id"])


    def updateTagmeta(self,postBody,id):
        query ={
            "id":id
        }
        url = config["api"]["meta"] + '/tagmeta/update?where=' + json.dumps(query)
        response = requests.post(url,json=postBody)
        tag = postBody["dataTagId"]

        if response.status_code == 200 or response.status_code == 204:
            
            print(f"{tag} Tagmeta updating successful..")

        else:
            print(f"{tag} Tagmeta updating unsuccessful..")
            print(response.status_code,response.content)

    
    def updateCalculations(self,postBody,id):
        query ={
            "id":id
        }
        url = config["api"]["meta"] + '/calculations/update?where=' + json.dumps(query)
        response = requests.post(url,json=postBody)
        tag = postBody["dataTagId"]
        if response.status_code == 200 or response.status_code == 204:
            
            print(f"{tag} Calculations updating successful..")

        else:
            print(f"{tag} Calculations updating unsuccessful..")
            print(response.status_code,response.content)

    # -------------------- Equipment level start ------------- #
    def createSumTagNameEL(self,postdf):
        try:
            dataTagId = postdf.loc[0,"dataTagId"]
            
            prefix = dataTagId.split("_")[0]
                
            unitsId = postdf.loc[0,"unitsId"][-4:]
            epqName = postdf.loc[0,"equipment"].replace(" ","")
            systemName = postdf.loc[0,"systemName"].replace(" ","")
            mp = postdf.loc[0,"measureProperty"].replace(" ","")
            
            sumTagName = prefix +"_" + unitsId + "_" + systemName + "_" + epqName + "_Total_Power_" + mp
            return sumTagName
        
        except:
            print(traceback.format_exc())


    def createPostBodyEL(self,sumTagName,postdf):
        try:
            instList = ["systemInstance","equipmentInstance","measureInstance"]

            for inst in instList:
                lim = postdf[inst].unique().min()
                postdf = postdf[postdf[inst]==lim].reset_index(drop=True)
            

            postBody = json.loads(postdf.to_json(orient="records"))[0]
            unitsId = postBody["unitsId"]
            postBody["dataTagId"] = sumTagName
            postBody["benchmarkLoad"] = {
                "status":"invalid"
            }
            postBody["table"] = {}
            postBody["measureProperty"] = "Equipment Apc"
            postBody["measureType"] = "Sum"
            postBody["description"] = postBody["equipment"] + " Total Equipment Apc"
            postBody["standardDescription"] = ""
            postBody["component"]  = ""
            postBody["subcomponent"]  = ""
            postBody["componentName"] = ""
            
            # print(json.dumps(postBody,indent=4))
            self.postInTagmeta(postBody)
            

        except:
            print(traceback.format_exc())


    def postInTagmetaEL(self,postdf):
        try:
            sumTagName = self.createSumTagNameEL(postdf)
            postBody = self.createPostBodyEL(sumTagName,postdf)
            return sumTagName
        except:
            print(traceback.format_exc())


    def createPostBodyForCal(self,sumTagName,postdf):
        publishBodyCal = {
            "type":"sum",
            "formula":{
            "v1":list(postdf["dataTagId"]),
            },
            "dataTagId":sumTagName,
            "unitsId":postdf.loc[0,"unitsId"]
        }
        # print("publishBodyCal")
        # print(publishBodyCal)
        return publishBodyCal
    


    def postInCal(self,sumTagName,postdf = {},postBodyCal = {}):
        # print("running posting function")
        if not postBodyCal:
            postBodyCal = self.createPostBodyForCal(sumTagName,postdf)

        checkBody = self.getCalculationsFromDataTagId(sumTagName)
        # print("checkbody")
        # print(json.dumps(checkBody,indent=4))

        if len(checkBody)>1:
            print("ALEART!!!!!!!!!!!!")
            print(f"found multiple traces of {sumTagName} in cal")
            return
        
        if len(checkBody) == 0:
            unitsId = postBodyCal["unitsId"]
            url = config["api"]["meta"] + f"/units/{unitsId}/calculations"
            # url = "http://13.251.5.125/exactapi/calculations"
            print(json.dumps(postBodyCal,indent=4))
            response = requests.post(url,json = postBodyCal)
            

            
            if response.status_code == 200 or response.status_code == 204:
                print(f"{sumTagName} Calcumations body posting successfull...")

            else:
                print(f"{sumTagName} calcumations body posting unsuccessfull...")
                print(response.status_code,response.content)
        else:
            # print(len(checkBody))
            # print(f"{sumTagName} is already present in calculation so updating...")
            postBodyCal["id"] = checkBody[0]["id"]
            # print(json.dumps(postBodyCal,indent=4))
            self.updateCalculations(postBodyCal,postBodyCal["id"])
            # print(json.dumps(postBodyCal,indent=4))

        print("*" * 60)


    # -------------------- Equipment level End ------------- #

    # ---------------------- system level Start --------------- #
    def createSumTagNameSL(self,namedf):
        try:
            dataTagId = namedf.loc[0,"dataTagId"]
            
            prefix = dataTagId.split("_")[0]

            unitsId = namedf.loc[0,"unitsId"][-4:]
            system = namedf.loc[0,"systemName"].replace(" ","")
            sysInst = str(namedf.loc[0,"systemInstance"])
            mp = namedf.loc[0,"measureType"].replace(" ","")
            
            sumTagName = prefix +"_"+ unitsId + "_" +system + "_" + sysInst + "_Total_Power_" + mp

            return sumTagName
        
        except:
            print(traceback.format_exc())


    def createPostBodySL(self,sumNameSL,pbdf):
        try:
            
            postBody = json.loads(pbdf.to_json(orient="records"))[0]
            unitsId = postBody["unitsId"]
            postBody["benchmarkLoad"] = {
                "status":"invalid"
            }
            postBody["table"] = {}
            postBody["dataTagId"] = sumNameSL
            postBody["measureProperty"] = "System Apc"
            postBody["measureType"] = "Sum"
            postBody["equipment"] = "Performance Kpi"
            postBody["equipmentName"] = "Performance Kpi"
            postBody["description"] = pbdf.loc[0,"systemName"].replace(" ","") + " Total System Apc"
            postBody["standardDescription"] = ""
            
            # print(json.dumps(postBody,indent=4))

            self.postInTagmeta(postBody)

        except:
            print(traceback.format_exc())


    def postInTagmetaSL(self,postdf):
        try:

            sumNameSL = self.createSumTagNameSL(postdf)
            self.createPostBodySL(sumNameSL,postdf)
            return sumNameSL

        except:
            print(traceback.format_exc())


    # ---------------------- system level End --------------- #

    # --------------------- Unit level start ---------------- #
    def createSumTagNameUL(self,namedf):
        try:
            dataTagId = namedf.loc[0,"dataTagId"]
            
            prefix = dataTagId.split("_")[0]

            unitsId = namedf.loc[0,"unitsId"][-4:]
            system = namedf.loc[0,"system"].replace(" ","")
            sysInst = str(namedf.loc[0,"systemInstance"])
            mp = namedf.loc[0,"measureProperty"].replace(" ","")

            sumTagName = prefix +"_" + unitsId + "_" + "Unit_Apc"
            return sumTagName
        
        except:
            print(traceback.format_exc())


    def createPostBodyUL(self,sumTagName,pbdf):
        try:
            
            postBody = json.loads(pbdf.to_json(orient="records"))[0]
            unitsId = postBody["unitsId"]
            postBody["benchmarkLoad"] = {
                "status":"invalid"
            }
            postBody["table"] = {}
            postBody["dataTagId"] = sumTagName
            postBody["measureProperty"] = "Unit Apc"
            postBody["measureType"] = "Sum"
            postBody["equipment"] = "Performance Kpi"
            postBody["equipmentName"] = "Performance Kpi"
            postBody["system"] = "Unit Performance"
            postBody["systemName"] = "Unit Performance"
            unitName = self.getUnitName(unitsId)
            postBody["description"] = unitName+ " Total Unit Apc"
            postBody["standardDescription"] = ""
            

            # print(json.dumps(postBody,indent=4))

            self.postInTagmeta(postBody)

        except:
            print(traceback.format_exc())


    def postinTagmetaUL(self,postdf):
        try:
            sumTagNameUL = self.createSumTagNameUL(postdf)
            self.createPostBodyUL(sumTagNameUL,postdf)
            return sumTagNameUL
        except:
            print(traceback.format_exc())


    # --------------------- Unit level End ---------------- #




class apcManager(posting):
    def __init__(self,unitsIdList):
        self.unitsIdList = unitsIdList


    def createCalPostBody(self,unitsId,dataTagId,formula,type):
        try:
            body = {
                "unitsId" : unitsId,
                "dataTagId" : dataTagId,
                "formula" : formula,
                "type" : type
            }
            return body
        except:
            tr()

            
    # --------------------- main steam flow by load start ------------------- #
    def createmsfBByLoadTagName(self):
        try:
            return self.loadTag[0]["dataTagId"] + "_" + self.mainSteamTag[0]["dataTagId"] + "_ratio"
        except:
            tr()


    def createmsfLoadTagmeta(self,sysNamedf,sysName,msfLoadTagName):
        try:
            postBody = json.loads(sysNamedf.to_json(orient="records"))[0]

            postBody["dataTagId"] = msfLoadTagName
            postBody["benchmarkLoad"] = {
                "status":"invalid"
            }
            postBody["table"] = {}
            postBody["measureUnit"] = self.ratioTagDict[self.unitsId][sysName][0]["measureUnit"] + "/" + self.ratioTagDict[self.unitsId]["load"][0]["measureUnit"]
            postBody["measureType"] =  "Ratio"
            postBody["measureProperty"] =  "ratio"
            postBody["description"] = "Specific Steam"
            postBody["system"] = "Generator System"
            postBody["systemName"] = "Generator System"
            postBody["equipmentName"] = "-"
            postBody["equipment"] = "-"
            
            self.postInTagmeta(postBody)

        except:
            tr()


    def createmsfLoadCal(self,msfLoadTagName):
        try:
            formula = {
                "v1": self.mainSteamTag[0]["dataTagId"],
                "v2" : self.loadTag[0]["dataTagId"]
            }
            
            postBody = self.createCalPostBody(self.unitsId,msfLoadTagName,formula,"ratio")

            self.postInCal(msfLoadTagName,postBodyCal=postBody)

        except:
            tr()



    def mainFuncmsfByLoad(self,sysNamedf,sysName):
        try:
            if len(self.mainSteamTag) > 0:
                msfLoadTagName = self.createmsfBByLoadTagName()
                self.createmsfLoadTagmeta(sysNamedf,sysName,msfLoadTagName)
                self.createmsfLoadCal(msfLoadTagName)
                return msfLoadTagName
        except:
            tr()
    # --------------------- main steam flow by load end ------------------- #


    # ---------------------------- current tag by main steam flow  start --------------------#
    def createCtMsfTagmeta(self,postdf,ctMsfTagName,sysName,type):
        try:
            postBody = json.loads(postdf.to_json(orient="records"))[0]

            postBody["dataTagId"] = ctMsfTagName
            postBody["benchmarkLoad"] = {
                "status":"invalid"
            }
            postBody["table"] = {}
            postBody["measureUnit"] = postBody["measureUnit"] + "/" + self.ratioTagDict[self.unitsId][sysName][0]["measureUnit"] 
            postBody["measureType"] = type
            postBody["measureProperty"] =  "Equipment Apc"
            postBody["description"] = postBody["equipment"] + " Apc Tph" 
            
            # pp(postBody)
            
            self.postInTagmeta(postBody)
        except:
            tr()


    def createCtMsfCal(self,ctMsfTagName,sumTagName,msfTag):
        try:
            formula = {
                "v1" : sumTagName,
                "v2" : msfTag
            }

            postBody = self.createCalPostBody(self.unitsId,ctMsfTagName,formula,"ratio")
            self.postInCal(ctMsfTagName,postBodyCal=postBody)
        except:
            tr()



    def createCtMsfCalV2(self,ctMsfMsfLoadTag,msfLoadTagName,ctMsfTagName):
        try:
            formula = {
                "v1" : ctMsfTagName,
                "v2" : msfLoadTagName
            }

            postBody = self.createCalPostBody(self.unitsId,ctMsfMsfLoadTag,formula,"product")
            pp(postBody)
            self.postInCal(ctMsfTagName,postBodyCal=postBody)
        except:
            tr()


    def mainFuncCtByMSF(self,postdf,sumTagName,sysName,msfLoadTagName):
        try:
            msfTag =self.ratioTagDict[self.unitsId][sysName][0]["dataTagId"]
            ctMsfTagName = msfTag + "_" + sumTagName + "_ratio"
            self.createCtMsfTagmeta(postdf,ctMsfTagName,sysName,"Ratio")
            self.createCtMsfCal(ctMsfTagName,sumTagName,msfTag)
            
            ctMsfMsfLoadTag = msfLoadTagName + "_" + ctMsfTagName + "_product"
            self.createCtMsfTagmeta(postdf,ctMsfMsfLoadTag,"load","Product")
            self.createCtMsfCalV2(ctMsfMsfLoadTag,msfLoadTagName,ctMsfTagName)
        except: 
            tr()
    # ---------------------------- current tag by main steam flow  end --------------------#

    # ---------------------------- current tag by load start ---------------------- #
    
    def mainFuncCtLoad(self,postdf,sumTagName):
        try:
            loadTag = self.ratioTagDict[self.unitsId]["load"][0]["dataTagId"]
            ctLoadTagName =  loadTag + "_" + sumTagName + "_ratio"

            self.createCtMsfTagmeta(postdf,ctLoadTagName,"load","Ratio")
            self.createCtMsfCal(ctLoadTagName,sumTagName,loadTag)
        except:
            tr()
    # ---------------------------- current tag by load end ---------------------- #


    # ------------------------- Creating meta level start ---------------------- #
    def mainELfunction(self):
        """
        Use: to filter data equipment wise.
        """
        try:
            print("At equipment level.....")
            tagmeta = self.getTagmetaFromUnitsId(self.unitsIdList)
            self.ratioTagDict = {}
            for unitsId in tagmeta:
                self.unitsId = unitsId
                loadQuery["unitsId"] = unitsId
                self.loadTag = self.getTagMeta(loadQuery)
                self.ratioTagDict[unitsId] = {}
                self.ratioTagDict[unitsId]["load"] = self.loadTag
                # pp(self.loadTag)

                df = pd.DataFrame(tagmeta[unitsId])
                print(df["systemName"].unique())

                for sysName in df["systemName"].unique():
                    if sysName != "Generator System":
                        
                        msfQuery["unitsId"] = unitsId
                        msfQuery["systemName"] = sysName
                        self.mainSteamTag = self.getTagMeta(msfQuery)
                        self.ratioTagDict[unitsId][sysName] = self.mainSteamTag
                        # pp(self.mainSteamTag)

                        sysNamedf = df[df["systemName"]==sysName]
                        msfLoadTagName = self.mainFuncmsfByLoad(sysNamedf,sysName)
        
                        for eqpName in sysNamedf["equipment"].unique():
                            eqpNamedf = sysNamedf[(sysNamedf["equipment"]==eqpName)].reset_index(drop=True)
                            
                            sumTagName = self.postInTagmetaEL(eqpNamedf)
                            self.postInCal(sumTagName,eqpNamedf)
                            
                            if len(self.mainSteamTag) > 0:
                                self.mainFuncCtByMSF(eqpNamedf,sumTagName,sysName,msfLoadTagName)
                            else:
                                self.mainFuncCtLoad(eqpNamedf,sumTagName)
        except:
            print(traceback.format_exc())


    def mainSLFunction(self):
        try:
            print("At system level......")
            tagmeta = self.getTagmetaForSL(self.unitsIdList)
            for unitsId in tagmeta:
                df = pd.DataFrame(tagmeta[unitsId])
                for sysName in df["systemName"].unique():
                    if sysName != "Generator System":
                        sysNamedf = df[df["systemName"]==sysName]
                        lim = sysNamedf["systemInstance"].unique().min()
                        sysNamedf = sysNamedf[sysNamedf["systemInstance"]==lim].reset_index(drop=True)

                        mainSteamTag = self.ratioTagDict[self.unitsId][sysName]
                        if len(mainSteamTag) > 0:
                            print("only taking product")
                            sysNamedf = df[(df["systemName"]==sysName) & (df["measureType"] == "Product")].reset_index(drop=True)
                        else:
                            print("only taking ratio")
                            sysNamedf = df[(df["systemName"]==sysName) & (df["measureType"] == "Ratio")].reset_index(drop=True)
                        # print(sysNamedf)
                        sumTagName = self.postInTagmetaSL(sysNamedf)

                        self.postInCal(sumTagName,sysNamedf)
                    
        except:
            print(traceback.format_exc())


    def mainULFucntion(self):
        try:
            print("At unit level......")
            tagmeta = self.getTagmetaForUL(self.unitsIdList)

            for unitsId in tagmeta:
                df = pd.DataFrame(tagmeta[unitsId])
                sumTagNameUL = self.postinTagmetaUL(df)
                self.postInCal(sumTagNameUL,df)

        except:
            print(traceback.format_exc())


    def createTagAndCalMeta(self):
        
        self.mainELfunction()
        self.mainSLFunction()
        self.mainULFucntion()


    # ------------------------- Creating meta level end ---------------------- #

    # ------------------------- Deleting meta Start --------------------------- # 
    def getDataTagIdFromMeta(self,tagmeta):
        if type(tagmeta) == list:
            dataTagIdLst= []
        
            for i in tagmeta:
                dataTagIdLst.append(i["dataTagId"])
            return dataTagIdLst

        elif type(tagmeta) == dict:
            dataTagIdLst= []
            lst = []
            for unitsId in tagmeta:
                for i in tagmeta[unitsId]:
                    dataTagIdLst.append(i["dataTagId"])
                    lst.append(i)
            return dataTagIdLst,lst


    def delDataInTagmeta(self,tagmeta,type):
        for i in tagmeta:
            dataTagId = i["dataTagId"]
            if type == "tagmeta":
                url = config["api"]["meta"] + "/tagmeta/" + i["id"]
            elif type =="cal":
                url = config["api"]["meta"] + "/calculations/" + i["id"]
                # print(url)
            else:
                print("procide valid type")
            
            response = requests.delete(url)

            if response.status_code == 200 or response.status_code == 204:
                if type == "tagmeta":
                    print(f"{dataTagId} deleting tagmeta successfull...")
                else:
                    print(f"{dataTagId} deleting calulations successfull...")


            else:
                if type == "tagmeta":
                    print(f"{dataTagId} deleting tagmeta unsuccessfull...")
                    print(response.status_code,response.content)
                else:
                    print(f"{dataTagId} deleting calulations unsuccessfull...")
                    print(response.status_code,response.content)




    def deleteTagAndCalMeta(self):
        tagmeta = self.getTagmetaForDel()
        dataTagIdLst = self.getDataTagIdFromMeta(tagmeta)
        tagmetaCal = self.getCalForDel(dataTagIdLst)

        self.delDataInTagmeta(tagmeta,"tagmeta")
        self.delDataInTagmeta(tagmetaCal,"cal")

    # ------------------------- Deleting meta End --------------------------- # 
    



