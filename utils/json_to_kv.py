#json_schema to KV
import sys, os
import pandas as pd
import json, re, copy

from itertools import groupby

LIST_SPLITTER='!|!'
get_type_str=lambda obj:str(type(obj)).replace("'",'').strip().split(' ')[1].replace('>','')

def detect_type(obj):
    ts=get_type_str(obj)
    obj_sz=len(str(obj))
    leaf=True
    if ts in ['dict', 'list', 'tuple']:
        obj_sz=len(obj)
        if ts=='dict':
            lst=list(obj.values())
        else:
            lst=obj
        d={}
        for rec in lst:
            t=get_type_str(rec)
            if not(t in d):d[t]=0
            d[t]+=1
            if t in ['dict', 'list', 'tuple']:
                leaf=False
        if len(d)==1:
            return (ts, t, 1, obj_sz, leaf)
        elif len(d)==0:
            return (ts, None, 0, obj_sz, leaf)
        else:
            d_items=list(d.items())
            d_items.sort(key=lambda x:x[0])
            strd='|'.join(['%s:%i'%(k,v) for k,v in d_items])
            return (ts, strd, len(d), obj_sz, leaf)
    return (ts, '', 0, obj_sz, leaf)

# def flatten(obj, key='', deep=0, Verbose=False):
#     olst=[]
#     if type(obj)==type({}):
#         if Verbose:print('\t'*deep, 'd', key, len(obj))
#         yield key, False
#         for k in obj.keys():
#             for kk, leaf in flatten(obj[k], k, deep=deep+1):
#                 kk=key+LIST_SPLITTER+kk
#                 #print('\t'*deep, 'd_kk', kk)
#                 yield kk, leaf
#     elif type(obj)==type([]):
#         if Verbose:print('\t'*deep, 'l', key, len(obj))
#         yield key, False
#         for i in range(len(obj)):
#             for kk, leaf in flatten(obj[i], i, deep=deep+1):
#                 kk=key+LIST_SPLITTER+kk
#                 #print('\t'*deep, 'l_kk', kk)
#                 yield kk, leaf
#     else:
#         yield key,True
    
def dget(d, klst):
    #This function and its sister dset recursively navigate a json to GET a value
    klst=copy.deepcopy(klst)
    k=''
    while k=='' and klst:
        k=klst.pop(0)
    if k!='':
        if type(d)==type([]):
            return dget(d[int(k)], klst)
        if k in d:
            return dget(d[k], klst)
        else:
            print('dget: key_notfound %s klst[n=%i] %s'%(k,len(klst), str(klst)[:100]))
            return 'notfound', []
    return d

def dset(d, klst, v):
     #This function and its sister dget recursively navigate a json to SET a value
    klst=copy.deepcopy(klst)
    k=''
    while k=='' and klst:
        k=klst.pop(0)
    if k!='':
        if type(d)==type([]):
            if len(klst)<1:
                d[int(k)]=v
                return v
            else:
                return dset(d[int(k)], klst, v)
        else:
            if len(klst)<1:
                d[k]=v
                return v
            else:
                return dset(d[k], klst, v)
    return None


def flatten(obj, key='', deep=0, Verbose=False):
    olst=[]
    if type(obj)==type({}):
        if Verbose:print('\t'*deep, 'd', key, len(obj))
        yield key, False
        for k in obj.keys():
            for kk, leaf in flatten(obj[k], k, deep=deep+1):
                kk=key+LIST_SPLITTER+kk
                #print('\t'*deep, 'd_kk', kk)
                yield kk, leaf
    elif type(obj)==type([]):
        if Verbose:print('\t'*deep, 'l', key, len(obj))
        yield key, False
        for i in range(len(obj)):
            for kk, leaf in flatten(obj[i], str(i), deep=deep+1):
                kk=key+LIST_SPLITTER+kk
                #print('\t'*deep, 'l_kk', kk)
                yield kk, leaf
    else:
        yield key,True
def json_all_keyvals(j, LeafsOnly=True, StringsOnly=False,  SortJoinKey=True, Verbose=False):
    olst=[];n=0
    for klst_str, leaf in flatten(j, Verbose=Verbose):
        n+=1
        if LeafsOnly and not(leaf):continue
        klst=klst_str.split(LIST_SPLITTER)
        v=dget(j, klst)
        if StringsOnly and type(v)!=type(''):continue
        olst.append([klst, v])
    if Verbose:
        print('json_all_keyvals: %i -> %i'%(n, len(olst)))
    if SortJoinKey:
        olst.sort(key=lambda x:len(x[0]))
    olst=[[LIST_SPLITTER.join(x[0]), x[1]] for x in olst]
    return olst






def schema_view(obj, prefix=None, printStringLists=True, depth=0, MAX_Depth=4, LIST_MAX=50):
    """Displays JSON objects"""
    prefix_tab=tab='\t'*depth
    if not(prefix is None):
        prefix_tab='%s[%s] '%(tab, str(prefix))
    typtup=detect_type(obj)
    typ, subtyp, num_subtyps, obj_sz, leaf=typtup
    if depth>MAX_Depth:
        return '%s maxdeep[%i] T:%s sz:%i val:%s'%(prefix_tab, depth, str(typtup), obj_sz, str(obj)[:20])
    olst=[];
    if num_subtyps==0 or subtyp is None:
        olst.append('%s(%s):%s'%(tab, typ, str(obj)[:20]))
    if  num_subtyps<8:
        if typ in ['list', 'tuple']:
            if subtyp=='str':
                if len(obj)<4:
                    olst.append('%s:%s'%(prefix_tab, str(obj)))
                elif printStringLists==True:
                    vlst=['\t%s%s'%(tab, s) for s in obj[:LIST_MAX]]
                    olst.append('%s:L[n=%i]\n%s'%(prefix_tab, obj_sz, '\n'.join(vlst)))
                else:
                    olst.append('%s:L[n=%i] trunc_str_val:%s'%(prefix_tab, obj_sz,  str(obj)[:20]))
            else:
                dlst=[]
                for i, rec in enumerate(obj):
                    dlst.append(schema_view(rec,
                                            prefix=i,
                                            printStringLists=printStringLists, 
                                            depth=depth+1, 
                                            MAX_Depth=MAX_Depth, LIST_MAX=LIST_MAX))
                    if i >LIST_MAX:break
                olst.append('%s:L[n=%i]\n%s'%(prefix_tab, obj_sz,  '\n'.join(dlst)))
        elif typ=='dict':
            kcount=0;dlst=[]
            for k, v in obj.items():
                if leaf:
                    s='\t%sk:%s:%s'%(tab, k, str(v))
                else:
                    s='\t%sk:%s:\n%s'%(tab, k, schema_view(v, 
                                                               printStringLists=printStringLists, 
                                                               depth=depth+1, 
                                                               MAX_Depth=MAX_Depth, LIST_MAX=LIST_MAX))
                dlst.append(s)
                kcount+=1
                if kcount >LIST_MAX:break
            olst.append('%s:D[n=%i]\n%s'%(prefix_tab, obj_sz, '\n'.join(dlst)))
    else:
        olst.append('%slf T:%s sz:%i str_val:%s'%(prefix_tab, str(typtup), obj_sz,  str(obj)[:20]))
    x='\n'.join(olst)
    if depth==0:  
        print('OUT:\n', x)
    return x

TypeSortDict={
    'bool':5,
    'numeric':4,
    'string':3,
    'list':2,
    'dict':1,
    'Flip':False
}


class bool_counter:
    def __init__(self, key=''):
        self.key=key
        self.lst=[]
        self.values=[]
        self.type='bool'
        self.n=0
    def get_values(self):
        return set(self.values)
    def sort_val(self):
        return (TypeSortDict[self.type], -self.n, self.key)
    def add(self, v):
        self.values.append(v)
        self.n+=1
        self.lst.append(v)
    def export(self, tab=''):
        if self.n==0:return 'num empty'
        s=sum(self.lst)
        return '%sbool n=%i true:%i '%(tab,self.n, s)
    
class numeric_counter:
    def __init__(self, key=''):
        self.key=key
        self.lst=[]
        self.values=[]
        self.type='numeric'
        self.n=0
    def get_values(self):
        return set(self.values)
    def sort_val(self):
        return (TypeSortDict[self.type], -self.n, self.key)
    def add(self, v):
        self.values.append(v)
        self.n+=1
        self.lst.append(v)
    def export(self, tab=''):
        if self.n==0:return 'num empty'
        return '%snum n=%i sz(%i, %i)'%(tab,self.n, min(self.lst), max(self.lst))
        
    
class string_counter:
    def __init__(self, key=''):
        self.key=key
        self.n=0
        self.type='string'
        self.values=[]
        self.lst=[]
    def get_values(self):
        return set(self.values)
    def sort_val(self):
        return (TypeSortDict[self.type], -self.n, self.key)
    def add(self, v):
        self.values.append(v)
        self.n+=1
        self.lst.append(v)
    def is_guid():
        pass
    def export(self, tab=''):
        len_lst=[len(x) for x in self.lst]
        if self.n==0:return 'str empty'
        ex_val=self.lst[0]
        if len(ex_val)>30:
            ex_val='%s<trunc>'%ex_val[:30]
        return 'string n=%i sz(%i, %i) example: %s'%(self.n, min(len_lst), max(len_lst),ex_val)
        

class list_counter:
    def __init__(self, key=''):
        self.key=key
        self.type_lst=[]
        self.values=[]
        self.type='list'
        self.n=0
        self.count_list=[0]
    def get_values(self):
        return self.values
    def sort_val(self):
        return (TypeSortDict[self.type], -self.n, self.key)
    def set_counts(self, count):
        while len(self.count_list)<=count:
            self.count_list.append(0)
        self.count_list[count]+=1
    def add(self, lst):
        self.n+=1
        type_tup=detect_type(lst)
        #('list', 'dict:1|int:1|list:1|str:1', 4, 4, False) (ts, strd, len(d), obj_sz, leaf)
        self.set_counts(type_tup[3])
        self.type_lst.append(type_tup)
        self.values.append(lst)
    def export(self, tab=''):
        if self.n==0:return 'list n=0'
        d={}
        len_lst=[]
        for type_tup in self.type_lst:
            typ=type_tup[0]
            obj_sz=type_tup[3]
            if not(typ in d):d[typ]=0
            d[typ]+=1
            len_lst.append(obj_sz)
        typeLst=list(d.items())
        typeLst.sort(key=lambda x:x[0])
        tstr='|'.join(['%s_%i'%(t,n) for t,n in typeLst])
        count_str='|'.join(['%i'%count for count in self.count_list])
        return 'list n=%i sz[%s] typeLst: %s'%(self.n, count_str, tstr)
    
class dict_counter:
    def __init__(self, key=''):
        self.key=key
        self.type_lst=[]
        self.values=[]
        self.n=0
        self.type='dict'
        self.sorted_key_List=None
        self.keytype_count_dict={}
        self.count_list=[0]
    def get_values(self):
        return self.values
    def sort_val(self):
        return (TypeSortDict['dict'], -self.n, self.key)
    def set_counts(self, count):
        while len(self.count_list)<=count:
            self.count_list.append(0)
        self.count_list[count]+=1
    def add(self, d):
        self.n+=1
        if d is None:return
        type_tup=detect_type(d)
        #('dict', 'dict:3|str:5', 2, 8, False) (ts, strd, len(d), obj_sz, leaf)
        self.type_lst.append(type_tup)
        self.set_counts(type_tup[3])
        self.values.append(d)
        for k, v in d.items():
            ts=get_type_str(v)
            if not(k in self.keytype_count_dict): self.keytype_count_dict[k]=[ts, 0]
            self.keytype_count_dict[k][1]+=1
    def get_keys(self):
        return list(self.keytype_count_dict.keys())
    def get_sorted_key_List(self):
        if not(self.sorted_key_List is None):
            return self.sorted_key_List
        else:
            return list(self.keytype_count_dict.keys())
    def key_export(self, tab=''):
        if self.n==0:return 'dict empty'
        len_lst=[x[0] for x in self.type_lst]
        ktlst=[[k] + self.keytype_count_dict[k] for k in self.get_sorted_key_List()]
        ktstr='\n'.join(['\t%s%s(%s)_%i'%(tab,k,t,n) for k,t,n in ktlst])
        return 'dict n=%i sz(%i, %i) keytypeLst:\n%s'%(self.n, min(len_lst), max(len_lst), ktstr)
    def export(self, tab=''):
        if self.n==0:return 'dict n=0'
        d={}
        len_lst=[]
        for type_tup in self.type_lst:
            typ=type_tup[0]
            obj_sz=type_tup[3]
            if not(typ in d):d[typ]=0
            d[typ]+=1
            len_lst.append(obj_sz)
        typeLst=list(d.items())
        typeLst.sort(key=lambda x:x[0])
        tstr='|'.join(['%s_%i'%(t,n) for t,n in typeLst])
        count_str='|'.join(['%i'%count for count in self.count_list])
        return 'dict n=%i sz[%s] typeLst: %s'%(self.n, count_str, tstr)
  
class data_counter:
    """part of jnode_counter"""
    def __init__(self, obj=None, key=''):
        self.d={}
        self.key=key
        self.set_key(key)
        self.n=0
        if obj:
            self.add(obj)
    def set_key(self, key=None):
        if key is None:
            key=self.key
        self.key=key
        for ts in self.d.keys():
            self.d[ts].key=key
    def sort_val(self):
        l=[self.d[k].sort_val() for k in self.type_keys()]
        if not(l):return (99999, -1, self.key)
        l.sort()
        if TypeSortDict['Flip']:l.reverse()
        return l[0]
    def add(self, obj, key=None):
        self.n+=1
        ts=get_type_str(obj)
        if ts in ['int', 'float']:
            if not(ts in self.d):
                self.d[ts]=numeric_counter(self.key)
            self.d[ts].add(obj)
        elif ts == 'str':
            if not(ts in self.d):
                self.d[ts]=string_counter(self.key)
            self.d[ts].add(obj)
        elif ts == 'dict':
            if not(ts in self.d):
                self.d[ts]=dict_counter(self.key)
            self.d[ts].add(obj)
        elif ts == 'list':
            if not(ts in self.d):
                self.d[ts]=list_counter(self.key)
            self.d[ts].add(obj)
        elif ts == 'bool':
            if not(ts in self.d):
                self.d[ts]=bool_counter(self.key)
            self.d[ts].add(obj)
        else:
            print("ERR data_counter:add >%s<"%ts, obj)
    def merge(self, other):
        for ts in other.d:
            if ts in self.d:
                for val in other.d[ts].values:
                    self.d[ts].add(val)
            else:
                self.d[ts]=other[ts]
    def get_vals(self):
        #All non(list, dict) values are sets
        d={}
        for ts in self.d.keys():
            d[ts]=self.d[ts].get_values()
        return d
    def get_dict_keys(self):
        if 'dict' in self.d:
            return self.d['dict'].get_keys()
        return []
    def type_keys(self):
        l=list(self.d.items())
        l.sort(key=lambda x:x[1].sort_val())
        if TypeSortDict['Flip']:l.reverse()
        return [x[0] for x in l]
    def key_export(self, tab='', sorted_keyLst=[]):
        if sorted_keyLst:
            self.d['dict'].sorted_key_List=sorted_keyLst
        return '||'.join([self.d[k].export(tab=tab) for k in self.type_keys()])
    def export(self, tab='', sorted_keyLst=None):
        if not(sorted_keyLst is None) and ('dict' in self.d):
            self.d['dict'].sorted_key_List=sorted_keyLst
        #return '|'.join(['%s[n=%i]'%(k, v.n) for k,v in self.d.items()])
        return '||'.join([self.d[k].export(tab=tab) for k in self.type_keys()])
            
class jnode_counter:
    """jnode_counter lets you analyze a set of jsons that are of the same type"""
    def __init__(self, obj=None, key='root', parent_predicate=(None,None), depth=0):
        self.parent=parent_predicate[0]
        self.parent_predicate=parent_predicate[1]
        self.depth=depth
        self.n=0
        self.occupancy=1.0
        self.children={}
        self.key=key
        self.set_key(key=key)
        self.data_counter=data_counter(key=key)
        self.obj=obj
        self.type=None
        if not(obj is None):
            self.add(self.obj)
        self.data_counter.set_key(key)
    def get_count(self):
        return self.data_counter.n
    def set_key(self, key=None):
        if key is None:
            key=self.key
        self.key=key
        self.key_path=key
        if self.parent:
            self.key_path='%s:%s'%(self.parent.key_path, str(key))
        for child in self.children.values():
            child.set_key()
    def is_list(self):
        if ('list' in self.data_counter.d):return True
        return False
    def get_vals(self, key_path):
        l=key_path.split(':')
        k=l.pop(0)
        #print(key_path, k, l, self.key, self.children.keys())
        if l:
            return self.children[k].get_vals(':'.join(l))
        return self.children[k].data_counter.get_vals()
    def get_types(self):
        return list(self.data_counter.d.keys())
    def sort_val(self):
        return self.data_counter.sort_val()
    def add(self, obj):
        typestr, typedictLst, num_types, obj_sz, leaf = detect_type(self.obj)
        typestr=get_type_str(obj)
        self.data_counter.add(obj)
        if typestr=='list':
            # if not('_list_' in self.children):
            #     self.children['_list_']=jnode_counter(obj=None, key='_list_', parent_predicate=(self, 'list_member'), depth=self.depth+1)
            # self.children['_list_'].add(obj)
            for idx, o in enumerate(obj):
                #='idx_%i'%idx
                k='idx_all'
                if not(k) in self.children:
                    self.children[k]=jnode_counter(obj=None, key=k, parent_predicate=(self, 'list_member'), depth=self.depth)
                self.children[k].add(o)
        elif typestr=='dict':
            for k in obj.keys():
                if not(k in self.children):
                    self.children[k]=jnode_counter(obj=None, key=k, parent_predicate=(self, 'dict_member'), depth=self.depth+1)
                self.children[k].add(obj[k])
        else:
            #print('ERRR jnode:add >%s<'%typestr, str(obj)[:20])
            return
        self.n=len(self.children)
    def merge(self, other):
        #This does not work use k='idx_all'during add function
        self.n+=other.n
        other.combine_list_members()
        self.data_counter.merge(other.data_counter)
        for k in other.children.keys():
            if (k in self.children):
                #self.children[k].combine_list_members()
                self.children[k].merge(other.children[k])
            else:
                self.children[k]=other.children[k]
    def combine_list_members(self):
        #This does not work use k='idx_all'during add function
        if 'idx_all' in self.children:return
        if not('idx_0' in self.children):return
        if not(self.is_list()):return
        idx_all =self.children['idx_0']
        del self.children['idx_0']
        list_keys=list(filter(lambda x:x.startswith('idx_'), self.children.keys()))
        if not(list_keys):return
        list_keys.sort()
        for idx_k in list_keys:
            print(self.key, 'merge', idx_k)
            idx_all.merge(self.children[idx_k])
            del self.children[idx_k]
        idx_all.set_key('idx_all')
        self.children['idx_all']=idx_all
        print('after_combine_list_members', self.children.keys())
    def iter_sorted_children_kv(self):
        l=[[k, v.sort_val()] for k, v in self.children.items()]
        l.sort(key=lambda x:x[1])
        if TypeSortDict['Flip']:l.reverse()
        for k, sortlst in l:
            yield [k, self.children[k]]
    def get_sorted_keys(self):
        return [x[0] for x in self.iter_sorted_children_kv()]
    def set_occupancy(self, N=None):
        if N is None:
            N=self.get_count()
        self.occupancy=self.get_count()/N if N>0 else 0.0
        for ch in self.children.values():
            ch.set_occupancy(N)
    def export(self, occupancy_thresh=0.0):
        #self.combine_list_members()#This does not work use k='idx_all'during add function
        if self.depth<1:
            self.set_occupancy()
        olst=[];klst=[]
        for k, v in self.iter_sorted_children_kv():
            #print(k, v.key_path, v.get_count(), v.occupancy)
            if v.occupancy<occupancy_thresh:continue
            olst.append(v.export(occupancy_thresh=occupancy_thresh))
            klst.append(k)
        ll=self.data_counter.get_dict_keys()
        sorted_klst=None
        if set(klst).issubset(set(ll)) or len(klst)==0:
            sorted_klst=klst
        tab='\t'*(self.depth)
        tail='\n'*max(0,2-self.depth)
        s = '%s[%s](%s)\n%s'%(tab, self.key_path, self.data_counter.export(tab='', sorted_keyLst=sorted_klst), '\n'.join(olst))
        s=s.replace('\n\n', '\n')+tail
        return s
    
    
    
    
#Guid Detection:
def parse_id(id):
    if not('/' in id):return {}
    if not(id.startswith('/')):return {}
    dd={}
    l=id.split('/')
    if not(len(id)>32):return {}
#     if len(l)%2==0:
#         dd['_valtype']='IDString'
#     else:
#         dd['_valtype']='IDString_uneven'
    for i in range(1, len(l)-1, 2):
        k=l[i]
        v=l[i+1]
        #print(i,k,v)
        dd[k]=v
    return dd

def detect_guid(s):
    if type(s)!=type('str'):return None
    if len(s)<36:return None
    #guid_regex= r"([a-fA-F\d]{8}-)(([a-fA-F\d]{4}-){3})([a-fA-F\d]{12})"
    guid_regex= r"([a-fA-F\d]{8}-)([a-fA-F\d]{4}-)([a-fA-F\d]{4}-)([a-fA-F\d]{4}-)([a-fA-F\d]{12})"
    g=re.findall(guid_regex, s)
    flatguid_regex= r"[a-fA-F\d]{32}"
    gf=re.findall(flatguid_regex, s)
    l=[]
    if (g):
        l.extend([''.join(x) for x in g])
    if (gf):
        l.extend([''.join(x) for x in gf])
    if l:
        return l
    else: return []
    
def detect_ids(key, s):
    if s.startswith('/'):
        d=parse_id(s)
        d['guids']={}
        for k in d.keys():
            if k in ['_id', 'guids', '_valtype']:continue
            g=detect_guid(d[k])
            if (g):
                d[k]=g
                d['guids'][k]=g
            else:
                d[k]=[d[k], 'str']
        return d
    else:
        g=detect_guid(s)
        if (g):
            return {'guids':{key:g[0]}, '_valtype':'guid_%i'%len(s)}
    return {}

def relabelIDs(j):
    j=copy.deepcopy(j)
    str_kvLst = json_all_keyvals(j, LeafsOnly=True, StringsOnly=True, Verbose=False)
    substitutionLst=[];guid_substLst=[]
    guid_proto_dict={}#'guid':'<id_guid>'
    for i, obj in enumerate(str_kvLst):
        kstr, s=obj
        klst=kstr.split(LIST_SPLITTER)
        key=klst[-1]
        gs=detect_ids(key, s)
        if gs and 'guids' in gs:
            gs=gs['guids']
            for val_key, guid in gs.items():
                if type(guid)==type([]):
                    guid=guid[0]
                if not(guid in guid_proto_dict):
                    proto='<%s_guid>'%val_key
                    p=1
                    while (proto in list(guid_proto_dict.values())):
                        proto='<%s_guid-%i>'%(val_key, p)
                        p+=1
                    guid_proto_dict[guid]=proto   
            substitutionLst.append([klst, [x[1] for x in gs.items()]])
            #print(substitutionLst[-1])
        else:
            pass;#print('\n\n')
    for klst, glst in substitutionLst:
        #print('klst',klst)
        oldv=v=dget(j, klst)
        changed=False
        for g in guid_proto_dict.keys():
            if g in v:
                proto=guid_proto_dict[g]
                v=v.replace(g, proto)
                changed=True
        if changed:
            dset(j, klst, v)
        #print('\n\n', klst, '\n', klst[-1], oldv, '\n', v)
    #print(guid_proto_dict)
    return j, guid_proto_dict


#json_schema discovery
lc_dict={};all_tables=[]
class jnode:
    """This is kinda deprecated superceded by schema_view""" 
    def __init__(self, key, obj, parent='', tab=''):
        self.parent=parent
        self.key=key
        self.obj=obj
        self.count=0
        self.cardinality=0
        self.type=str(type(obj))
        self.leaf=False
        if not(('dict' in self.type)or('list' in self.type)):
            self.leaf=True
        self.tab=tab
        self.dict_children=[]
        self.list_children=[]
        self.compute_path(parent)
        self.deep()
    def compute_path(self, parent):
        parent_path=''
        if parent:
            parent_path=parent.path
            if 'list' in parent.type:
                self.path=parent_path+"[%s]"%self.key
            else:
                self.path=parent_path+"['%s']"%self.key
        else:
            self.path=parent_path+"['%s']"%self.key
    def compute_clean_path(self):
        if self.parent and self.parent.compute_clean_path():
            parent_path=self.parent.compute_clean_path()
            if 'list' in self.parent.type:
                path=parent_path+"|%s"%self.key
            else:
                path=parent_path+"|%s"%self.key
        else:
            path=self.key
        return path
    def deep(self):
        #Fills out node representation
        if 'dict' in self.type:
            for key in self.obj.keys():
                self.dict_children.append(jnode(key, self.obj[key], parent=self, tab=self.tab+'\t'))
        if 'list' in self.type:
            for idx, obj in enumerate(self.obj):
                self.list_children.append(jnode('%i'%idx, obj, parent=self, tab=self.tab+'['))
        self.cardinality=len(self.dict_children)+len(self.list_children)
    def xprint_(self, string, fp=None):
        if fp:
            fp.write("%s\t%s\n"%(self.path, string))
        else:
            print(string)
    def print(self, string, fp=None):
        if self.leaf:
            k=self.key
            leaf_val=self.value()
            sz=len(leaf_val)
        else:
            k=''
            leaf_val = ''
            sz=0
        lc_key =self.key.lower()
        if lc_key in lc_dict:
            db_loc = '\t'.join([lc_dict[lc_key]['actual_field'], leaf_val, str(sz), lc_dict[lc_key]['mapping_str']])
        else:
            db_loc='\t'.join([k, leaf_val, str(sz), ''])
        if fp:
            s=u"%s"%string#.encode('utf-8', strict='ignore')
            fp.write("%s\t%s\t%s\n"%(self.path, db_loc, s))
            # try:
            #     fp.write("%s\t%s\t%s\n"%(self.path, db_loc, s))
            # except:
            #     print(s)
        else:
            print(string)
    def value(self):
        return str(self.obj).replace('\n', ' ').replace('\r', ' ')
    def display(self, fp=None, LL=False):
        if LL:
            LL= ['path', 'key', 'key_val'] + ['level_%i'%i for i in range(14)]
            if fp:
                fp.write('\t'.join(LL)+'\n')
            return LL
        if self.leaf:
            val=self.value()
            self.print('%s%s (%s) Leaf:"%s"'%(self.tab, str(self.key), self.type, val), fp=fp)
            return
        self.print('%s%s (%s) [n=%i]'%(self.tab, self.key, self.type, self.cardinality), fp=fp)
        for n in self.dict_children:
            n.display(fp=fp)
        for n in self.list_children:
            n.display(fp=fp)
    def to_dict(self):
        d={}
        if self.leaf:
            d[self.key]=self.value()
            return d
        for n in self.dict_children:
            d.update(n.to_dict())
        for n in self.list_children:
            d.update(n.to_dict())
        return d
    def to_flat_dict(self, key_lambda=lambda x:x.compute_clean_path()):
        #all lists converted to str
        d={}
        if self.leaf:
            d[key_lambda(self)]='%s'%self.value()
            return d
        for n in self.dict_children:
            if 'class' in str(type(n)):
                d.update(n.to_flat_dict())
            else:
                print(type(n), n)
                d[key_lambda(self)]='dictC: %s'%str(n)
        if self.list_children:
            d[key_lambda(self)]='listC: %s'%str([str(c.value()) for c in self.list_children])
        return d
    def to_fullpath_dict(self):
        d={}
        if self.leaf:
            d[self.path]=self.value()
            return d
        for n in self.dict_children:
            d.update(n.to_fullpath_dict())
        for n in self.list_children:
            d.update(n.to_fullpath_dict())
        return d
    def __str__(self):
        v=str( self.value())
        l=len(v)
        if l>100:
            v="%s_trunc[sz:%i]"%(v[:100], l)
        return "%s key:%s dict_children:%i list_children:%i val:%s"%(self.path, 
                                                                     self.key, 
                                                                     len(self.dict_children), 
                                                                     len(self.list_children), 
                                                                     v)
#     def display_2(self, fp=None):
#         if self.leaf:
#             self.print('%s%s (%s) Leaf:"%s"'%(self.tab, str(self.key), self.type, str(self.obj)), fp=fp)
#             return
#         self.print('%s%s (%s) [n=%i]'%(self.tab, self.key, self.type, self.cardinality), fp=fp)
#         for n in self.dict_children:
#             n.display(fp=fp)
#         for n in self.list_children:
#             n.display(fp=fp)
    def compact(self):
        pass


def read_table_schema(inpathLst, opath):
    d={}
    for inp in inpathLst:
        fn=os.path.basename(inp)
        buff= open(inp, 'r').read()
        cluster_name='Nrp.%s'%fn.split('.')[1]
        for b in buff.split('\n'):
            if not(b):continue
            if 'List' in b:continue 
            if 'temp' in b:continue
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
    LL=['table','field', 'type']+all_tables
    oLst=[]
    lc_dict={}
    for k in d.keys():
        for field in d[k].keys():
            l=[0 for x in range(len(all_tables))]
            for table_map in d[k][field]['mapping']:
                l[all_tables.index(table_map)]+=1
            lc_field=field.lower()
            if not(lc_field in lc_dict):
                d[k][field]['mapping'].sort()
                mapping_str=LIST_SPLITTER.join(d[k][field]['mapping'])
                lc_dict[lc_field]={'actual_field':field, 'idx_mapping':l, 'table_hits':len(d[k][field]['mapping']), 'mapping_str':mapping_str}
            oLst.append([k,field,d[k][field]['type']]+l)
    df = pd.DataFrame(oLst, columns=LL)
    df.to_csv(opath)
    return d, lc_dict, all_tables

if __name__ == '__main__':
    class class_x:
        def __init__(self):
            pass
        def RR(c):
            return 66

    lst=[1.0, 'wgh', 2, {}, [], ['sds'], [2,3], [2,3, 'kjk'], {'s':67}, {'g':[232,2]}, {'g':[232,2], 'l':8988},class_x(),class_x, RR, None]
    for v in lst:
        print(type(v), detect_type(v))
"""
if __name__ == '__main__':
	ud=r"C:\\Users\\dosuciu\\Documents\\git\\ds_pyutils\\test_data\\"
	fn="ARMCorrelationIdSearch&b1bfee44-d937-4f44-bc73-8fab982ed6db.json"
	json_path=ud+fn
    root=os.path.basename(json_path)
    J=jnode(root, json.loads(open(json_path, 'r', encoding ='utf8').read()))
    fp=open(ud+'schema_%s_dbloc.txt'%os.path.basename(json_path), 'w')
    J.display(LL=True, fp=fp)
    J.display(fp=fp)
    fp.close()"""