def schema_view(j, printStringLists=False, depth=0, MAX_Depth=4, LIST_MAX=50):
    """Displays JSON objects"""
    tab='\t'*depth
    t=type(j)
    if depth>MAX_Depth:
        s=str(j)
        return '%s%s sz:%i trunc d%i:%s'%(tab,t,len(s), depth, s[:20])
    olst=[];pp='??'
    if 'dict' in str(t):
        ki=0;d_sz=len(j)
        for k in j.keys():
            ki+=1
            v=j[k]
            vtype=str(type(v));obj_sz=0
            dict_prefix='%sD[%s](%i/%i)'%(tab, k, ki, d_sz)
            tdict_key_prefix='\t'
            if 'dict' in vtype:
                olst.append(dict_prefix)
                obj_sz=len(v)
                pp='dict:dict'
                olst.append('%s\n%s'%(tdict_key_prefix, schema_view(v, printStringLists=printStringLists, depth=depth+1, MAX_Depth=MAX_Depth)))
            elif 'list' in vtype:
                olst.append(dict_prefix)
                pp='dict:list'
                d={};rtype=''
                #Compute List type
                for rec in v:
                    rtype=str(type(rec))
                    if not(rtype in d):d[rtype]=0
                    d[rtype]+=1
                if (len(d)==1) and ('str' in rtype) and printStringLists:
                    #Print List of strings
                    vlst=['\t%s%s'%(tab, rec) for rec in v[:LIST_MAX]]
                    olst.append('%s:L[n=%i]\n%s'%(tdict_key_prefix, len(v), '\n'.join(vlst)))
                elif (len(d)==1) and ('dict' in rtype):
                    #Dict->List of Dicts
                    dlst=[]
                    for i, rec in enumerate(v):
                        dlst.append(schema_view(rec, printStringLists=printStringLists, depth=depth+1, MAX_Depth=MAX_Depth))
                        if i >LIST_MAX:break
                    olst.append('\n'.join(['\t%s'%x for x in dlst]))
                else:
                    #list of some other type
                    olst.append('%s: types%s ex:%s'%(tdict_key_prefix, str(list(d.items())), str(v)[:20]))

            else:
                olst.append('%s val:%s'%(dict_prefix, str(v)[:20]))
                pp='dict' 
            if ki >LIST_MAX:break
    elif ('list' in str(t)):
        #List of ??Types??
        pp='list';obj_sz=len(j);dlst=[]
        for i, rec in enumerate(j):
            #print('input-list [n=%i]'%len(j), i, str(rec)[:10])
            dlst.append(schema_view(rec, printStringLists=printStringLists, depth=depth+1, MAX_Depth=MAX_Depth))
            if i >LIST_MAX:break
        olst.append('%sL[n=%i] %s'%(tab, obj_sz, '\n'.join(dlst)))
    else:
        olst.append('%sLeaf(%s) sz:%i val:%s'%(tab, str(type(j)), len(str(j)),  str(j)[:20]))
        pp='other'
    if not(olst):
        print('huh: ', str(j)[:20])
    #print('pp', pp, 'olst', str(olst)[:20])
    x='\n'.join(olst)
    if depth==0:  
        print('OUT:\n', x)
    return x