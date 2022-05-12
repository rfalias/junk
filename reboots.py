import kernel_check
import signal
import functools
import asyncio
import concurrent.futures
import time
import argparse
import salt
from filesystemUtilities import loadYamlFromLocalDisk


quit = False

def log(caller, msg):
    print(f"[ {caller} ] - {msg}")


def checks(glob, spec, step, cmdstep):
    rtime = 10
    if "retrytime" in spec[step]:
        rtime = spec[step]['retrytime']
    try:
        success = True
        salt_cmds = spec[step][cmdstep]
        local = salt.client.LocalClient()
        log(glob, f"Validating {step} {cmdstep} {salt_cmds}")
        for s in salt_cmds:
            if quit:
                return False
            # Can't pop a dict entry during iter
            # Cmds can have a retry, so get just the first key so we don't try to run 'retry'
            # Don't question it, it just works
            for cmd in [list(s.keys())[0]]:
                if quit:
                    return False
                desired_value = s[cmd]
                retry = 1
                if 'retry' in s.keys():
                    retry = s['retry']
                    log(glob, f"Will retry {cmd} {retry} times")
                else:
                    log(glob, f"No retry specified, running {cmd} once")
                for rt in range(retry):
                    if quit:
                        return False
                    salt_run = local.cmd(f"{glob}", cmd)
                    log(glob, f"Salt Return Data {salt_run}")
                    if len(salt_run) <= 0:
                        log(glob, f"Validation step `{cmd}` failed. Got no return")
                        success = False
                    desired = str(desired_value).lower()
                    actual = str(salt_run).lower()
                    if desired not in actual:
                        log(glob, f"Pre-Validation step `{cmd}` Failed. {desired} not in {actual}")
                        success = False
                        log(glob, f"Sleeping for {rtime}")
                        time.sleep(rtime)
                    if success:
                        break
            if not success:
                log(glob, "Command failed, ending validation")
                return success
        if quit:
            log(glob, "Asked to exit")
            return False
        log(glob, f"Finally finished, result: {success}")
        return success
    except Exception as e:
        log(glob, e)
        return False


def reboot_group(group, caller, spec):
    local = salt.client.LocalClient()
    for server in group.keys():
       if quit:
           log(caller, f"Asked to stop")
           return False
       log(server, "Running pre checks")
       check_status = checks(server, spec, 'pre', 'salt')
       log(server, f"Check status {check_status}")
       if not check_status:
           log(server, f"Check status failed, skipping host group {caller}")
           return True
       try:
           log(caller, f"Calling reboot on {server}")
           ping = local.cmd(f"{server}", 'test.ping')
           log(caller, f"{ping}")
       except Exception as e:
           log(caller, f"Server failed to reboot with: {e}")
           return False
       log(server, f"Reboot complete, running post checks")
       check_status = checks(server, spec, 'post', 'salt')
    log(caller, f"Group {caller} completed")
    return True


def raise_graceful_exit(*args):
    log("Global", "Asking reboots to stop after this iteration")
    global quit
    quit = True
    exit()


def do_reboot(host, spec):
    kernel = spec['kernel']
    hostglob = host
    signal.signal(signal.SIGINT, raise_graceful_exit)
    signal.signal(signal.SIGTERM, raise_graceful_exit)
    log("Global", "Gathering servers requiring kernel updates")
    log("Global", f"{kernel} - {hostglob}")
    c = kernel_check.check(kernel, hostglob)
    if len(c) <= 0:
        log("Global", "No reboots needed for this host group")
        exit()
    log("Global", "Obtained reboot groups")
    with concurrent.futures.ThreadPoolExecutor() as ex:
        tasklist = []
        for k in c:
            if len(k) > 0:
                calling_group = list(k.keys())[0][:8]
                log("Global", f"Starting group reboot {list(k.keys())[0][:8]}")
                tasklist.append(ex.submit(reboot_group, k, calling_group, spec))
        for future in concurrent.futures.as_completed(tasklist):
            log("Global", f"{future} thread completed {future.result}")

if __name__ == "__main__":
    config = loadYamlFromLocalDisk('groups.yml')
    for host in config.keys():
        kernel = config[host]['kernel']
        print(f"Checking {host} - Target Kernel: {kernel}")
        spec = config[host]
        do_reboot(host, spec)
