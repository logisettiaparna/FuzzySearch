from flask import Flask
from flask import request
from flask_restful import Api, Resource, reqparse
import paramiko
import fnmatch
import pandas as pd
import time
from easygui import msgbox
import matplotlib.pyplot as plt
from matplotlib import pyplot
from uncertainties import ufloat
import plotly.plotly as py
import plotly.graph_objs as go


import pysparnn.cluster_index as ci
from xml.dom import minidom
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer

plt.style.use('seaborn-whitegrid')

app = Flask(__name__)
api = Api(app)

higvaluedetect="<alert><issue>High values of Lab numbers Detected</issue><message>Next best action required</message></alert>"
lowvaluedetect="<alert><issue>Low values of Lab numbers Detected</issue><message>Next best action required</message></alert>"
perfectvaluedetect="<message>Lab results are perfect.</message>"

@app.route('/optum/hackathon')
def api_hello():
    mbrid=request.args.get('Memberid')
    date=request.args.get('DateOfService')
    user=request.args.get('User')
    # policyNum=request.args.get('policyNum', default = '')
    # SubscriberId=request.args.get('SubscriberId', default = '')

    COMP = "dbslp0569"
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(COMP, username="xxx", password="xxxxxxxx", allow_agent = False)

    tv = TfidfVectorizer()
    ftp = ssh.open_sftp()

    common_message=[]
    abnormal_array = []
    labvalues_array=[]
    testdesc_array=[]
    hist_labvalues_array=[]
    hist_testdesc_array=[]
    highnormal_array=[]
    dob_array=[]
    gender_array=[]
    stdin, stdout, stderr=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_labresults.sh " + str(mbrid) + " "+ str(date))
    for line in stdout.read().splitlines():
        #print(line)
        if(str(line).__contains__("0 row(s)")):
            common_message.append("404 Error. Member NOT Found")
            restxml="404 Error. Member NOT Found"
            restxml="<ALERT>"+restxml+"</ALERT>"
            resp = app.make_response(restxml)
            resp.headers['Content-Type'] = 'text/xml'
            return resp
        if(str(line).__contains__("ci:abnl_cd")):
                abnormal_array.append(str(line).split("=")[3].replace("'",""))
        if(str(line).__contains__("ci:rslt_nbr")):
                labvalues_array.append(str(line).split("=")[3].replace("'",""))
        if(str(line).__contains__("ci:tst_desc")):
                testdesc_array.append(str(line).split("=")[3].replace("'",""))
        if(str(line).__contains__("ci:hi_nrml")):
                highnormal_array.append(str(line).split("=")[3].replace("'",""))

    if(str(abnormal_array).__contains__("H")):
        print(str(abnormal_array))
        print("High Values detected...With result of :::"+str(labvalues_array)+"for Lab procedure:::"+str(testdesc_array))
        stdin1, stdout1, stderr1=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_member.sh " + str(mbrid))
        for line1 in stdout1.read().splitlines():
            if(str(line1).__contains__("ci:DOB")):
                dob_array.append(str(line1).split("=")[2].replace("'",""))
            if(str(line1).__contains__("ci:gender")):
                gender_array.append(str(line1).split("=")[2].replace("'",""))
    ###Get All hist
        dob=str(dob_array).replace("[","").replace("]","").replace("'","")
        print("Raw DOB:"+dob)
        dob1=2018-int(dob)
        print("uflaot value::"+str(dob1))
        gen=str(gender_array).replace("[","").replace("]","").replace("'","")
        ####Next Action Plan - Unsupervised
        testsite_array=[]
        for file in ftp.listdir('/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/Hackathon_Lookup_BestAction'):
            if fnmatch.fnmatch(file, '*bestactions'):
                print(file)
                with ftp.open("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/Hackathon_Lookup_BestAction/"+file) as f:
                    for row in f:
                        testsite_array.append(row)
        tv.fit(testsite_array)
        features_vec = tv.transform(testsite_array)
        cp = ci.MultiClusterIndex(features_vec, testsite_array)
        search_data = ["High"+" "+str(dob1)+str(testdesc_array)+" "+gen]
        search_features_vec = tv.transform(search_data)
        retval_raw=str(cp.search(search_features_vec, k=1, k_clusters=2, return_distance=False)).split("|")[3]
        print(str(retval_raw))

        #Get History
        stdin2, stdout2, stderr2=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_hist_labresults.sh " + str(mbrid))
        for line2 in stdout2.read().splitlines():
            if str(line2).__contains__("ci:rslt_nbr"):
                hist_labvalues_array.append(str(line2).split("=")[3].replace("'",""))
            if(str(line2).__contains__("ci:tst_desc")):
                hist_testdesc_array.append(str(line2).split("=")[3].replace("'",""))
#pyplot.show()
        restxml="<bestActions>"+str(retval_raw)+"</bestActions>"+"<age>"+str(dob1)+"</age>"+"<gender>"+str(gender_array).replace("[","").replace("]","").replace("'","")+"</gender>"+"<CurrentLabvalues>"+str(labvalues_array)+"</CurrentLabvalues><TestDescriptions>"+str(testdesc_array)+"</TestDescriptions><HistLabvalues>"+str(hist_labvalues_array)+"</HistLabvalues><HistTestDescriptions>"+str(hist_testdesc_array)+"</HistTestDescriptions>"
        resp = app.make_response(higvaluedetect.replace("><",">\n<")+"\n"+restxml.replace("><",">\n<"))
        resp.headers['Content-Type'] = 'text/xml'
        return resp
    if(str(abnormal_array).__contains__("L")):
        print(str(abnormal_array))
        print("Low Values detected...With result of :::"+str(labvalues_array)+"for Lab procedure:::"+str(testdesc_array))
        stdin1, stdout1, stderr1=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_member.sh " + str(mbrid))
        for line1 in stdout1.read().splitlines():
            if(str(line1).__contains__("ci:DOB")):
                dob_array.append(str(line1).split("=")[2].replace("'",""))
            if(str(line1).__contains__("ci:gender")):
                gender_array.append(str(line1).split("=")[2].replace("'",""))
                ###Get All hist
        dob=str(dob_array).replace("[","").replace("]","").replace("'","")
        print("Raw DOB:"+dob)
        dob1=2018-int(dob)
        print("uflaot value::"+str(dob1))
        gen=str(gender_array).replace("[","").replace("]","").replace("'","")
        ####Next Action Plan - Unsupervised
        testsite_array=[]
        for file in ftp.listdir('/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/Hackathon_Lookup_BestAction'):
            if fnmatch.fnmatch(file, '*bestactions'):
                print(file)
                with ftp.open("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/Hackathon_Lookup_BestAction/"+file) as f:
                    for row in f:
                        testsite_array.append(row)
        tv.fit(testsite_array)
        features_vec = tv.transform(testsite_array)
        cp = ci.MultiClusterIndex(features_vec, testsite_array)
        search_data = ["High"+" "+str(dob1)+str(testdesc_array)+" "+gen]
        search_features_vec = tv.transform(search_data)
        retval_raw=str(cp.search(search_features_vec, k=1, k_clusters=2, return_distance=False)).split("|")[3]
        print(str(retval_raw))
        stdin2, stdout2, stderr2=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_hist_labresults.sh " + str(mbrid))
        for line2 in stdout2.read().splitlines():
            if str(line2).__contains__("ci:rslt_nbr"):
                hist_labvalues_array.append(str(line2).split("=")[3].replace("'",""))
            if(str(line2).__contains__("ci:tst_desc")):
                hist_testdesc_array.append(str(line2).split("=")[3].replace("'",""))
                #pyplot.show()
        restxml="<bestActions>"+str(retval_raw)+"</bestActions>"+"<age>"+str(dob1)+"</age>"+"<gender>"+str(gender_array).replace("[","").replace("]","").replace("'","")+"</gender>"+"<CurrentLabvalues>"+str(labvalues_array)+"</CurrentLabvalues><TestDescriptions>"+str(testdesc_array)+"</TestDescriptions><HistLabvalues>"+str(hist_labvalues_array)+"</HistLabvalues><HistTestDescriptions>"+str(hist_testdesc_array)+"</HistTestDescriptions>"
        resp = app.make_response(lowvaluedetect.replace("><",">\n<")+"\n"+restxml.replace("><",">\n<"))
        resp.headers['Content-Type'] = 'text/xml'
        return resp
    if(not(str(abnormal_array).__contains__("H")) and not(str(abnormal_array).__contains__("L")) and str(abnormal_array).__contains__("'")):
        print(str(abnormal_array))
        print("Fit Values:::"+str(labvalues_array)+"for Lab procedure:::"+str(testdesc_array))
        stdin1, stdout1, stderr1=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_member.sh " + str(mbrid))
        for line1 in stdout1.read().splitlines():
            if(str(line1).__contains__("ci:DOB")):
                dob_array.append(str(line1).split("=")[2].replace("'",""))
            if(str(line1).__contains__("ci:gender")):
                gender_array.append(str(line1).split("=")[2].replace("'",""))
                ###Get All hist
        dob=str(dob_array).replace("[","").replace("]","").replace("'","")
        print("Raw DOB:"+dob)
        dob1=2018-int(dob)
        print("uflaot value::"+str(dob1))
        # stdin2, stdout2, stderr2=ssh.exec_command("/mapr/datalake/ODM/mleccm/prd/c360/p_temp/ANI/HACKThon_Hive_Script/hbase_hist_labresults.sh " + str(mbrid))
        # for line2 in stdout2.read().splitlines():
        #     if str(line2).__contains__("ci:rslt_nbr"):
        #         hist_labvalues_array.append(str(line2).split("=")[3].replace("'",""))
        #     if(str(line2).__contains__("ci:tst_desc")):
        #         hist_testdesc_array.append(str(line2).split("=")[3].replace("'",""))
                #pyplot.show()
        restxml="<age>"+str(dob1)+"</age>"+"<gender>"+str(gender_array).replace("[","").replace("]","").replace("'","")+"</gender>"+"<CurrentLabvalues>"+str(labvalues_array)+"</CurrentLabvalues><TestDescriptions>"+str(testdesc_array)+"</TestDescriptions>"
        resp = app.make_response(perfectvaluedetect.replace("><",">\n<")+"\n"+restxml.replace("><",">\n<"))
        resp.headers['Content-Type'] = 'text/xml'
        return resp

    stdin.close()
    ftp.close()


app.run(debug=True)
