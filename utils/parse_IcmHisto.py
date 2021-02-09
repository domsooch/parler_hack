import sys, os, random
import pandas as pd

b= open(r'C:\Users\dosuciu\OneDrive - Microsoft\Monitoring_NRP\nrp_errors.csv', 'r',encoding='utf-8').read()
lst=b.split('\n')

errb= [x.split('\t') for x in open('ErrorCodes.txt', 'r',encoding='utf-8').read().split('\n')]
err_dict=dict(errb)

errdict={}
oLst=[]
i=0
for V in [x.split(',') for x in lst]:
    if not(V):continue
    if len(V) <8:
        print('badline', V)
        continue
    v=V[0].lower().replace(' ','').replace('_','')
    ed={};l=[]
    for k in err_dict.keys():
        if k.lower() in v:
            l.append((k,1))
            #print(i, v, k.lower(), len(l))
            if not(k in errdict):errdict[k]=0
            errdict[k]+=1
    ed['errTitle']=V[0].replace(',','')
    ed['start_date']=V[6]
    ed['end_date']=V[7]
    ed['IncidentId']=V[3]
    ed['inc_count']=V[5]
    ed['inc_type']=V[4]
    d=dict(l)
    ed.update(d)
    ed['n']=len(d)
    
    oLst.append(ed)
    i+=1
    
df=pd.DataFrame(oLst)
df.to_csv('errmapping.csv')
    


print(errdict)

