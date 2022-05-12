import os
import json
import salt.client

def check(target_version, glob):
    local = salt.client.LocalClient()
    kern = local.cmd(glob, 'cmd.run', ['uname -a'])
    kern = {k: kern[k] for k in sorted(kern)}
    tlist = {}
    kern_groups = {}
    for k in kern.keys():
        gkey = k[:8]
        if gkey in kern_groups.keys():
            kern_groups[gkey][k] = kern[k]
        else:
            kern_groups[gkey] = {k: kern[k]}
    from packaging import version
    tgt = version.parse(target_version)
    boot_list = list()
    for k in kern_groups.keys():
        hold_dict = dict(kern_groups[k])
        for gkey in kern_groups[k]:

            uname = kern_groups[k][gkey]
            kernelv = version.parse(uname.split()[2])

            if tgt <= kernelv:
                hold_dict.pop(gkey, None)
        if len(hold_dict) > 0:
            boot_list.append(hold_dict)
    return boot_list


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--kver', help='Target Kernel Version', required=True)
    parser.add_argument('--glob', help='Host Glob', required=True)
    args = parser.parse_args()
    c = check(args.kver, args.glob)
    for k in c:
        print("Reboot Group")
        for s in k.keys():
            print(f"{s} - {k[s]}")
