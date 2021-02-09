import sys, os, json
from importlib import reload
import os, sys, json, glob, copy
import sys, os, random
import pandas as pd
import re, datetime, fnmatch

from . import json_to_kv as JK

column_headers=[
    'resourceGroup',
    'resourceType',
    'name',
    '_metadata_key',
    'MigrationPhase',
    'resourceGuid',
    'fullName',
    'lastOperationId',
    'lastOperationType',
    'createdTime',
    'lastModifiedTime',
    'reconciliationState',
    'groupName',
    'RNM_keys_num',
    'RNM_keys',
    'RNM_guids',
    'RNM_guid_types',
    'etag',
    'subscriptionId',
    'moveOperationState',
    'isCrossSubscription',
    'migrationPhase',
    'properties',
    'properties_trunc_30000',
    'properties_trunc_60000',
    'subid',
    'fn',
    'state',
    'registrationDate',
    'limits',
    'allowThrottleCrpOperations',
    'isGlobalTagsAllowedForSql',
    'isGlobalTagsAllowedForStorage',
    'isAppGwGatewaySubscription',
    'isExrGwGatewaySubscription',
    'isVpnGwGatewaySubscription',
    'isSecureGwGatewaySubscription',
    'isVMSSVirtualNetworkGatewayAllowed',
    'tags',
    'location',
    'sku',
    'zones',
    'internalZones',
    'providerName',
]

def parseNRP(ud, sub_fn, rnm_fn, root_name):
    print('parseNRP:', ud, sub_fn, root_name)
    jpath=ud+sub_fn
    rgLst=[]
    df, unused_serviceIds=MatchNRPSubToRNM(jpath, root_name, rnm_path=ud+rnm_fn)
    J=JK.jnode('root', json.loads(open(jpath, 'r').read()))
    opath=ud+'NRP_rnmmatch_%s.csv'%root_name
    fp=open(opath, 'w')
    J.display(LL=True, fp=fp)
    J.display(fp=fp)
    fp.close()


def guidDict(j):
    J=JK.jnode('_',j)
    dd=J.to_dict()
    guid_dict = {}
    for k, v in dd.items():
        if len(v)==36:
            guid_dict[v]=k
    return dd, guid_dict, list(guid_dict.keys())

def import_rnm_output(inp):
    #Output of rnm client run
    #b=open(inp, 'r').read()
    with open(inp, 'r', encoding="utf8", errors='ignore') as f:
        b = f.read().replace('\x00', '')
    result = re.findall(r'Resource Id:(.*) Type:(.*)\n', b)
    print('found: %i recs'%len(result))
    rdict=dict(result)
    return [x[0] for x in result], rdict

def MatchSubToRNM(json_path, rgLst, serviceIDLst):
    olst=[]
    fn=os.path.basename(json_path)
    j=json.loads(open(json_path, 'r').read())
    i=0;
    for resrc in j:
        try:
            key=resrc['metadata']['key']
            #print(i, key)
        except:
            print('Bad thing!!!@', resrc)
            continue
        V=copy.deepcopy(resrc['value'])
        V['subid']=subid
        V['fn']=fn
        _d, guid_dict, vguidLst = guidDict(resrc)
        rnm_keys=[]
        rnm_guids=[]
        for guid in vguidLst:
            if guid in serviceIDLst:
                rnm_keys.append(guid_dict[guid])
                rnm_guids.append(guid)
        V['RNM_keys_num']=len(rnm_guids)
        V['RNM_keys']='|'.join(rnm_keys)
        V['RNM_guids']='|'.join(rnm_guids)
        if 0:
            for rg in rgLst:
                if rg in key.lower():
                    V=copy.deepcopy(V)
                    V['_metadata_key']=key
                    #V['rnm_command']='echo "YoYoYo: %s";rnmclient /rnm:%s.rnm.core.windows.net:15000 grfs:%s'%(subid,region, subid)
                    for k in V.keys():
                        V[k] = str(V[k])[:30000]
                    olst.append(V)
        if 1:
            V=copy.deepcopy(V)
            V['_metadata_key']=key
            #V['rnm_command']='echo "YoYoYo: %s";rnmclient /rnm:%s.rnm.core.windows.net:15000 grfs:%s'%(subid,region, subid)
            for k in V.keys():
                V[k] = str(V[k])[:30000]
            olst.append(V)
        i+=1
    df=pd.DataFrame(olst)
    df.to_excel('%s.xls'%jpath, sheet_name="subTornm")
    return df

def get_name(r):
    if 'name' in r:
        return r['name']
    if 'name' in r['value']:
        return r['value']['name']
    return r['value']['resourceGuid']
key_to_idx=lambda k:int(k.split('][')[1])

def entityDict(j, nameLst=[]):
    J=JK.jnode('_',j)
    dd=J.to_fullpath_dict()
    entity_dict = {}
    for k, v in dd.items():
        jidx=key_to_idx(k)
        for name in v.split('/'):
            if name:
                if nameLst and (name in nameLst):
                    if not(name in entity_dict):entity_dict[name]={'idx':jidx, 'lst':[]}
                    entity_dict[name]['lst'].append(k)
    return entity_dict

def GenerateResourceDependencyMap(j, rsrcLst):
    col_set=set([])
    nameLst=[get_name(j[i]) for i in range(len(j))]
    edict=entityDict(j, nameLst)
    for rIdx in range(len(nameLst)):
        name=rsrcLst[rIdx]['name']
        if not(edict[name]['idx'] == rIdx):
            pass#print('ERROR', edict[name], name, len(nameLst), len(edict))
        name_idx=edict[name]['idx']
        clst=[]
        for path in edict[name]['lst']:
            didx = key_to_idx(path)
            dependency_key='%03d_%s_%s'%(didx, rsrcLst[didx]['resourceType'], rsrcLst[didx]['name'])
            if not(dependency_key in rsrcLst[rIdx]):
                rsrcLst[rIdx][dependency_key]=[]
                col_set.add(dependency_key)
                clst.append(dependency_key)
            rsrcLst[rIdx][dependency_key].append(path)
        for dep in clst:
            rsrcLst[rIdx][dep]='\r\n'.join(rsrcLst[rIdx][dep])[:30000]
    return rsrcLst, col_set

def GenerateResourceDependencyMap_RDFE(j, rsrcLst):
    col_set=set([])
    nameLst=[j[i]['RowKey'] for i in range(len(j))]
    edict=entityDict(j, nameLst)
    for rIdx in range(len(nameLst)):
        name=rsrcLst[rIdx]['RowKey']
        if not(edict[name]['idx'] == rIdx):
            pass#print('ERROR', edict[name], name, len(nameLst), len(edict))
        name_idx=edict[name]['idx']
        clst=[]
        for path in edict[name]['lst']:
            didx = key_to_idx(path)
            dependency_key='%03d_%s_%s'%(didx, rsrcLst[didx]['resourceType'], rsrcLst[didx]['resourceName'])
            if not(dependency_key in rsrcLst[rIdx]):
                rsrcLst[rIdx][dependency_key]=[]
                col_set.add(dependency_key)
                clst.append(dependency_key)
            rsrcLst[rIdx][dependency_key].append(path)
        for dep in clst:
            rsrcLst[rIdx][dep]='\r\n'.join(rsrcLst[rIdx][dep])[:30000]
    return rsrcLst, col_set

def organizeLL(LL, entityLst, entitytype_lambda=lambda x:x['resourceType']):
    LL_type_dict={}
    group_set=set()
    for e in entityLst:
        group = entitytype_lambda(e)
        group_set.add(group)
        for k in e.keys():
            if e[k].strip()=='':continue
            if not(k in LL_type_dict):LL_type_dict[k]={'count':0, 'group_set':set()}
            LL_type_dict[k]['count']+=1
            LL_type_dict[k]['group_set'].add(group)
    groupLst=list(group_set)
    groupLst.sort()
    lltypeLst=list(LL_type_dict.items())
    single_groupLL=list(filter(lambda x:len(x[1]['group_set'])==1, lltypeLst))
    outLL=list(filter(lambda x:len(x[1]['group_set'])>1, lltypeLst))
    outLL.sort(key=lambda x:-x[1]['count'])
    outLL=[x[0] for x in outLL]

    for group in groupLst:
        if not(group):continue
        glst=[x[0] for x in filter(lambda x:group in x[1]['group_set'], single_groupLL)]
        glst.sort()
        outLL.extend(glst)
        #print('group: %s  group_specific: %i'%(group, len(glst)), glst)
    return outLL
        
def ParseRDFE(ud, rdfe_fn, rnm_fn, run_root):
    serviceIDLst=[]
    if os.path.exists(ud+rnm_fn):
        rnm_path=ud+rnm_fn
        serviceIDLst, rnm_guidtype_dict = import_rnm_output(rnm_path)

    with open(ud+rdfe_fn, 'r') as f:
        b=f.read().split('=== <Tsq> ===')
    print(len(b), 'numtsq')
    entityLst=[];rnmLL=set([]);LL=set([]);LLdict={}
    ii=0
    for r in b:
        ii+=1
        dlst=[]
        for l in r.split('\n'):
            if ':' in l:
                ll=l.split(':')
                if len(ll)>2:
                    k=ll[0]
                    v=':'.join(ll[1:])
                else:
                    k,v=ll[:2]
                k=k.strip()
                v=v.strip()
                
                if k=='RowKey':
                    l=v.split('-')
                    resourceType=l[0]
                    name='-'.join(l[1:])
                    #print(ii,k,'rowkey', v)
                    dlst.append(('resourceType',resourceType))
                    dlst.append(('resourceName',name))
                    dlst.append(('RowKey',v)) 
                i=0
                for rnm_guid in serviceIDLst:
                    v=v.strip()
                    if rnm_guid.strip() in v:
                        rnmk="%s_rnm_%i"%(k,i)
                        dlst.append((rnmk,"%s:%s"%(rnm_guidtype_dict[rnm_guid], rnm_guid)))
                        rnmLL.add(rnmk)
                        i+=1
                LL.add(k)
                dlst.append((k,v))
        for k,v in dlst:
            if not(k in LLdict):LLdict[k]=0
            if v or v==0:
                LLdict[k]+=1
        entityLst.append(dict(dlst))
    migration_LL=list(filter(lambda x:'migrat' in x.lower(), LL))
    migration_LL.sort()
    migration_LL=['my_MigrationState']+migration_LL
    firstLL=['RowKey', 'resourceType', 'resourceName']+migration_LL
    for f in firstLL:
        if f in LLdict:
            del LLdict[f]
    for e in entityLst:
        for k in migration_LL:
            if k in e:
                if e[k].strip()!='':
                    e['my_MigrationState']='migrating'
                    break
    print('len(entityLst)', len(entityLst))
    sub_dict=entityLst.pop(0)
    llLst=list(LLdict.items())
    llLst.sort(key=lambda x:-x[1])
    LL=[k for k,v in llLst]
    LL=organizeLL(LL, entityLst)
    
    
    oLst=list(filter(lambda x:('RowKey' in x) and x['RowKey'].strip()!='', entityLst))
    col_lst=[]
    olst, col_set=GenerateResourceDependencyMap_RDFE(oLst, oLst)
    col_lst = list(col_set)
    col_lst.sort()
    print('MatchSubToRNM: %i entities'%len(olst))
    
    df=pd.DataFrame(olst, columns=firstLL+LL+list(rnmLL)+col_lst)
    df.to_csv(ud+'RDFE_subToRNM_%s.csv'%run_root)
    return df, olst
