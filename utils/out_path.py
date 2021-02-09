import os, sys, datetime, fnmatch, glob


def TimeCode():
    """
    generates a string timecode.
    :return:
    """
    now = datetime.datetime.now()
    fn_timr_root = "{:%Y%m%dT%H%M}".format(now)
    return fn_timr_root

UDLST =[r"C:\Users\dosuciu\Desktop\icm_work\\",
        r"C:\\Users\dosuciu\\OneDrive - Microsoft\\OnCallResources\\IcmCases\\"]

def make_out_dir(udLst=None):
    if udLst is None:
        udLst=UDLST
    ud=None
    for ud in udLst:
        if os.path.exists(ud):
            break
    if ud is None:
        ud=os.getcwd()
        print('JustUsing cwd(): %s'%ud)
    outud=os.path.join(ud, 'out_py\\')
    os.makedirs(outud,exist_ok=True)
    print ('work_path: %s out_py: %s %i files in dir'%(ud, outud, len(os.listdir(outud))))
    return ud, outud

if __name__=='__main__':
    ud, out_ud = make_out_dir()