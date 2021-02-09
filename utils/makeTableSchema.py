import os, sys, glob
import pandas as pd

ud= r'C:\\Users\\dosuciu\\OneDrive - Microsoft\\DataBaseTables\\'
fnLst=glob.glob(ud+"kusto_tables\*.txt")#['Nrp.mdsnrp.Tables.txt', 'Nrp.binrp.Tables.txt']


d={}
for pth in fnLst:
    buff= open(pth, 'r').read()
    fn=os.path.basename(pth)
    cluster_name='Nrp.%s'%fn.split('.')[1]
    for b in buff.split('\n'):
        if not(b):continue
        if 'List' in b:continue
        b=b.replace('.create-merge table','').strip()
        table_name = b[:b.find('(')].strip()
        flst = b[b.find('(')+1:b.find(')')].split(', ')
        fdict=dict([(n,{'type':t,'mapping':[]}) for n,t in [x.split(':')[:2] for x in flst]])
        cltab_name='%s.%s'%(cluster_name, table_name)
        print(cltab_name)
        d[cltab_name] = fdict
for k in d.keys():
    table = d[k]
    for field in table.keys():
        for kk in d.keys():
            if field in d[kk]:
                table[field]['mapping'].append(kk)
all_tables=list(d.keys())  
all_tables.sort()  
LL=['table','field', 'type', 'counts']+all_tables
oLst=[]
for k in d.keys():
    for field in d[k].keys():
        l=[0 for x in range(len(all_tables))]
        for table_map in d[k][field]['mapping']:
            l[all_tables.index(table_map)]+=1
        oLst.append([k,field,d[k][field]['type'], sum(l)]+l)
df = pd.DataFrame(oLst, columns=LL)
df.to_csv(ud+'tables.csv')


