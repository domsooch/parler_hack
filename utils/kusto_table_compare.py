import os, sys, datetime, json
import os, sys, glob, re, copy, math, json, random
import pandas as pd
import numpy as np

def percent_max_threshold(lst, pm_thresh=0.0, Random=False):
    if Random:
        lst=[random.random() for i in range(len(lst))]
    a=np.array(lst)
    pm_a=a/a.max()
    blst=pm_a[:]>=pm_thresh
    ilst=np.arange(a.shape[0])[blst]
    print('percent_max_threshold(%i, pm_thresh=%0.2f)->%i'%(len(lst), pm_thresh, len(ilst)))
    return blst, ilst

#Deprecate
def compute_percent_threshold(vals, perc_threshold_value, return_type='val'):
    thresh_val=max(vals)*perc_threshold_value
    if return_type==thresh_val:
        return thresh_val
    idxLst=[xx[0] for xx in filter(lambda x:x[1]>thresh_val, enumerate(vals))]
    if return_type=='bool':
        olst=[False for i in range(len(vals))]
        for i in idxLst:
            olst[i]=True
        return olst
    return idxLst
        
#deprecate
"""
def top_list(inlst, val_lam=lambda x:x, perc=0.1, perc_threshold_value=None, returnIdx=False):
    l=[(i, val_lam(x)) for i,x in enumerate(inlst)]
    if perc_threshold_value:perc=perc_threshold_value
    if perc>0:
        l.sort(key=lambda x:-x[1])
    else:
        l.sort(key=lambda x:x[1])
    if perc_threshold_value==None:
        num_recs=int(len(l)*abs(perc))
        l=l[:num_recs]
        l.sort(key=lambda x:x[0])
        idxLst=[r[0] for r in l]
        perccut_str='percent_cut:%0.2f'%perc
    else:
        assert(perc_threshold_value>=0)
        vals=[x[1] for x in l]
        thresh_val=max(vals)*perc_threshold_value
        idxLst=[r[0] for r in  list(filter(lambda x:x[1]>=thresh_val, l))]
        perccut_str='perc_thresh_val:%0.2f'%thresh_val
    
    olst=[inlst[i] for i in idxLst]
    num_recs=len(idxLst)
    print('top_list: inLst[n=%i] %s to %i'%(len(inlst), perccut_str, num_recs))
    if returnIdx:
        return olst, idxLst
    return olst

#deprecate
class counter:
    def __init__(self):
        self.n=0
        self.count_dict={}
    def add(self, k):
        if not(k in self.count_dict):self.count_dict[k]=0
        self.n+=1
        self.count_dict[k]+=1
    def to_list(self):
        l=list(self.count_dict.keys())
        l.sort()
        return l
    def top_list(self, perc=0.1):
        l=list(self.count_dict.items())
        if perc>0:
            l.sort(key=lambda x:-x[1])
        else:
            l.sort(key=lambda x:x[1])
        num_recs=int(len(l)*abs(perc))
        print('counter:top_list: n=%i perc_cut:%0.2f set:%i to %i'%(self.n, perc, len(self.count_dict), num_recs))
        return l[:num_recs]
"""
    
    
def load_tables(glob_pathORList, select_clusterLst=[], select_tableLst=[], Verbose=True, FlattenTableName=False, LowerCaseLeafs=False):
    #Loads Kusto Table Schemas
    if type(glob_pathORList)==type(''):
        fnLst=glob.glob(glob_pathORList)
    else:
        fnLst=glob_pathORList
    cm_pattern=re.compile(r".create-merge table (.*) \((.*)\)", re.MULTILINE | re.DOTALL)
    cdt_dict={};flat_dict={}
    for pth in fnLst:
        if Verbose:print('load_tables: %s'%os.path.basename(pth))
        buff= open(pth, 'r').read()
        fn=os.path.basename(pth)
        ff=fn.split('.')
        if len(ff)>3:
            ff=[ff[0], '%s.%s'%(ff[1], ff[2]), ff[3]]
        cluster_name, db_name, xx = ff
        if select_clusterLst:
            if not(cluster_name in select_clusterLst):continue
        if Verbose:print(cluster_name)
        if not(cluster_name in cdt_dict):cdt_dict[cluster_name]={}
        if not(db_name in cdt_dict[cluster_name]):cdt_dict[cluster_name][db_name]={}
        if Verbose:print('\t', db_name)
        for b in buff.split('\n'):
            if not(b):continue
            if not('.create-merge' in b):continue
            if 'with (' in b:
                b=b[:b.find('with')].strip()
            mfind=cm_pattern.findall(str(b))
            if not(mfind):
                print(fn, 'FAIL PARSE: %s'%b)
                continue
            mfind=mfind[0]
            table_name = mfind[0].strip()
            if 'temp' in table_name.lower(): continue
            if select_tableLst:
                if not(table_name in select_tableLst):
                    #print('\t\t', table_name, str(select_tableLst))
                    continue
            if Verbose:print('\t\t', table_name)
            schema_lst = mfind[1].strip().split(', ')
            flst=list(filter(lambda x:not('[' in str(x[0])), [x.split(':')[:2] for x in schema_lst]))
            #flst=[x.split(':')[:2] for x in schema_lst]
            #fdict=dict([(n,{'type':t,'mapping':[]}) for n,t in flst])
            #fdict=[n for n,t in flst]
            if LowerCaseLeafs:
                flst=[[x[0].lower(), x[1]] for x in flst]
            if FlattenTableName:
                flat_dict['%s|%s|%s'%(cluster_name,db_name,table_name)]=flst
            cdt_dict[cluster_name][db_name][table_name]=flst
    if FlattenTableName:
        return flat_dict
    return cdt_dict


def group_count(l, lam = lambda x:x):
    d={}
    for x in l:
        v=lam(x)
        if not(v in d):d[v]=0
        d[v]+=1
    dl=list(d.items())
    dl.sort(key=lambda x:-x[1])
    print('val, count')
    for v,c in dl:
        print(v,c)
        
def choose_tables(full_table_name, qc_db, allowed_table_names=None, disallowed_table_names=['manticore']):
    table_tagLst=['request', 'entry', 'front', 'gate', 'qos', 'etw','event', ]
    cluster_name, db, tbl_name=full_table_name.split('|')
    if not(db in qc_db[cluster_name]):
        return False
    allowed_tables_by_count=[x[0] for x in filter(lambda x:x[1]>1000,  qc_db[cluster_name][db].items())]
    label=tbl_name.lower()
    if disallowed_table_names:
        for n in disallowed_table_names:
            if n in label: return False
    if (label in allowed_tables_by_count):
        if allowed_table_names:
            for tag in allowed_table_names:
                if tag in label:return True
            return False
        else:
            return True
    return False

def get_leafs(d, tab='', child_lam=lambda x:'list' in str(type(x)), get_lam=lambda d,k:[r[0] for r in d[k]]):
    olst=[]
    for k in d.keys():
        #print(tab, k)
        if child_lam(d[k]):
            olst.extend(get_lam(d,k))
        else:
            l=get_leafs(d[k], tab=tab+'\t', child_lam=child_lam, get_lam=get_lam)
            olst.extend(l)
    return olst

def PrepareTableMatrix(cdt_dict, querycounts_db, Table_percMaxThresh=0.0, Field_percMaxThresh=0.0, allowed_table_names=None, disallowed_table_names=['manticore']):
    table_dict=dict(get_leafs(cdt_dict, tab='', child_lam=lambda x:'list' in str(type(x)), 
                   get_lam=lambda d,k:[(k,set([r[0] for r in d[k]]))]))
    tableLst=[]
    for ftn in table_dict.keys():
        if choose_tables(ftn, querycounts_db, allowed_table_names=allowed_table_names, disallowed_table_names=disallowed_table_names):
            tableLst.append(ftn)
    
    tablefield_dict=dict([[t, table_dict[t]] for t in tableLst])
    print('PrepareTableMatrix:', len(table_dict), len(tableLst), len(tablefield_dict))
    group_count(tablefield_dict.keys(),lam=lambda x:x.split('|')[0])
    
    fldLst=[]
    for table in tablefield_dict.keys():
        fldLst.extend( tablefield_dict[table])

    #Build TableField_Matrix
    tbls=tableLst
    flds=list(set(fldLst))

    sm=[[0 for f in range(len(flds))] for t in range(len(tbls))]
    for f in range(len(flds)):
        field=flds[f]
        for t in range(len(tbls)):
            table=tbls[t]
            table_fieldLst=tablefield_dict[table]
            if field in table_fieldLst:
                sm[t][f]=1
    sm=np.array(sm)
    print('naive: sm.shape: ', sm.shape)
    #Thresh Fields
    sum_cols=sm.sum(axis=0)[:]
    blst, ilst= percent_max_threshold(sum_cols, pm_thresh=Field_percMaxThresh)
    sm=sm[:,blst]
    flds=np.array(flds)[blst]
    print('field_thresh: sm.shape', sm.shape)

    #Thresh Tables
    sum_rows=sm.sum(axis=1)[:]
    blst, ilst= percent_max_threshold(sum_rows, pm_thresh=Table_percMaxThresh, Random=True)
    sm=sm[blst,:]
    tabls=np.array(tbls)[blst]
    print('field/table_thresh: sm.shape', sm.shape)
    _tabls=tabls
    df=pd.DataFrame(sm, columns=flds, index=_tabls)
    return df


