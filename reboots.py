import sys
import logging
import kernel_check
import signal
import functools
import asyncio
import concurrent.futures
import time
import argparse
import salt
import salt_bus
from filesystemUtilities import loadYamlFromLocalDisk

''' Global bool to stop parallel execution'''
quit = False

def log(src, str):
    """Basic logger, just prints for now

    Args: 
        src: source thread name
        str: string to print

    Returns:
        None
    """

    print(f"[{src}] - {str}")


def checks(glob, spec, step, cmdstep, dry = True):
    """Runs specified checks via salt.
    Uses salt's cmd.run or calls a salt module with arguments

    Args: 
        glob: The target host name
        spec: YAML derived dict that contains the checks to run
        step: either pre or post, simply used as a lookup for commands
        cmdstep: Derived from yaml spec, either salt or cmd
        dry: Dry run or not, used to filter out commands marked as safe

    Returns:
        bool
    """
    
    step_val = spec.get(step, False)
    
    if not step_val:
        log(glob, f"No step {step} in {spec.keys()} - No {step}-checks. Skipping")
        return True
        
    rtime = step_val.get('retrytime', 10)
    stepcheck = step_val.get(cmdstep, False)
    
    if not stepcheck:
        return True
        
    try:
        # Setup some defaults, salt, etc....
        success = True
        salt_cmds = step_val.get(cmdstep, None)
        local = salt.client.LocalClient()
        log(glob, f"Validating {step} {cmdstep} {salt_cmds}")
        # For every command listed in either pre or post in the yaml file
        for s in salt_cmds:
            # Just check if we need to exit due to sigint
            if quit:
                return False
                
            # Can't pop a dict entry during iter
            # Cmds can have a retry, so get just the first key so we don't try to run 'retry', 'optional', or other non cmd keys
            # Don't question it, it just works
            for cmd in [list(s.keys())[0]]:
                if quit:
                    return False
                # Map the yaml desired value from the cmd key
                desired_value = s[cmd]
                # Check if this command is flagged as safe
                safe = s.get('safe', False)
                # If the command is marked as safe, and it's a dry run, skip entirely but continue to other commands
                if safe and dry:
                    log(glob, f"`{cmd}` is marked as safe and will be skipped during dry runs")
                    break
                # Get more possible yaml values, set defaults otherwise
                args = s.get('args',[])
                optional = s.get('optional', False)
                retry = s.get('retry', 1)
                log(glob, f"Will retry `{cmd}` {retry} times")
                # Do the commands until we hit retry max
                for rt in range(retry):
                    # Assume we are successful at first, try commands and fail if we fail
                    success = True
                    log(glob, f"Attempting validation step `{cmd}`")
                    if quit:
                        return False
                    # If the step is 'salt' run a salt module, with optional arguments
                    if cmdstep == 'salt':
                        salt_run = local.cmd(f"{glob}", cmd, args)
                    else:
                    # Otherwise it's a cmd.run
                        salt_run = local.cmd(f"{glob}", 'cmd.run', [cmd])
                    # Something went wrong and salt didn't return anything, drop out
                    if len(salt_run) <= 0:
                        log(glob, f"Validation step `{cmd}` failed. Got no return")
                        # Optional flagged commands tolerate failures, so set it to false if we are not an optional flag
                        if not optional:
                            success = False
                    # Get our desired val and actual values from salt
                    desired = str(desired_value).lower()
                    actual = str(salt_run).lower()
                    # We didn't find a simple string in string match, so mark as failed
                    if desired not in actual:
                        log(glob, f"Validation step `{cmd}` Failed. String '{desired}' not in '{actual}'")
                        # Optional flagged commands tolerate failures
                        if not optional:
                            success = False
                        log(glob, f"Sleeping for {rtime}")
                        # Retry is set to more than 1, so sleep to the set time and run again
                        if retry > 1:
                            time.sleep(rtime)
                    # Success! Log it and break out, we are done here in the retry loop
                    if success:
                        if optional:
                            log(glob, f"Validation step `{cmd}` passed.  `{desired}` was NOT found in `{actual}` but {cmd} is optional")
                            break
                        log(glob, f"Validation step `{cmd}` passed.  `{desired}` was found in `{actual}`")
                        break
            # We encountered a failure, so exit asap. Commands need to be successful to move on
            if not success:
                log(glob, f"Command failed, ending validation")
                return success
        # Check global quit flag
        if quit:
            log(glob, f"Asked to exit")
            return False
        # Return the ultimate result of all commands
        return success
    except Exception as e:
        log(glob, f"e")
        return False


def reboot_group(group, caller, spec, dry = True):
    """Reboots specified groups with a defined yaml.

    Args: 
        group: The target host name
        spec: yaml definitions
        caller: The source thread group name
        dry: Simulate or now

    Returns:
        bool
    """
    # Setup salt
    local = salt.client.LocalClient()
    
    # Check servers in the provided group
    for server in group.keys():
        # Set our reboot wait timer, default is 5 minutes. Set it longer for physical servers
        reboot_wait = spec.get('reboot_wait', 300)
        log(server, f"Reboot wait set to {reboot_wait}")
        if quit:
           log(server, f"Asked to stop")
           return False
        log(server, f"Running salt pre checks")
        # Run salt pre checks
        check_status = checks(server, spec, 'pre', 'salt', dry)
        log(server, f"Check status {check_status}")
        # Something happened with pre checks, so drop this group entirely
        # Groups need to execute in order, as they should be defined as a cluster that needs safe reboots
        if not check_status:
           log(server, f"Salt Check status failed, skipping host group [{caller}]")
           return True
        log(server, f"Running cmd pre checks")
        # Running cmd pre checks
        check_status = checks(server, spec, 'pre', 'cmd', dry)
        log(server, f"Cmd Check status {check_status}")
        # Something went wrong with cmd checks, drop group
        if not check_status:
           log(server, f"Check status failed, skipping host group [{caller}]")
           return True
        # pre checks successful, reboot if it's not a dry run
        try:
           if dry:
               log(server, f"Calling DRY RUN reboot on [{server}]")
               ping = local.cmd_async(f"{server}", 'test.ping')
               log(server, f"DRY RUN SAMPLE {ping}")
               log(server, f"Set dry run salt bus timeout to 1 second")
               reboot_wait = 1
           else:
               log(server, f"Confirmed, performing real reboot on [{server}]")
               # Run async, we will watch event bus to find the server up status
               reboot_command = local.cmd_async(f"{server}", 'system.reboot')
           log(server, f"Async reboot scheduled, awaiting minion_start on event bus for {reboot_wait} seconds")
           # Waits a specified time for a server to enter minion_start on the salt event bus
           await_status = salt_bus.wait_for_start(f"{server}", reboot_wait)
           # We didn't get a return so the host wasn't detected as alive. Fail this group
           if not await_status and not dry:
               log(server, f"minion_start timed out on event bus, could not detect a host startup")
               return False
           if dry:
               log(server, f"minion_start timed out on event bus, dry run. This is expected")
        except Exception as e:
           log(server, f"Server failed to reboot with: {e}")
           return False

        log(server, f"Reboot complete, running post checks")
        log(server, f"Running salt post checks")
        # Post salt checks
        check_status = checks(server, spec, 'post', 'salt', dry)
        log(server, f"Check status {check_status}")

        if not check_status:
           log(server, f"Salt Check status failed, skipping host group [{caller}]")
           return False
        # Post cmd checks
        log(server, f"Running cmd post checks")
        check_status = checks(server, spec, 'post', 'cmd', dry)
        log(server, f"Cmd Check status {check_status}")
        if not check_status:
           log(server, f"Check status failed, skipping host group [{caller}]")
           return False
    log(caller, f"Group  completed")
    return True

# Flag an exit condition
def raise_graceful_exit(*args):
    log("global", "Asking reboots to stop after this iteration")
    global quit
    quit = True
    exit()


def do_reboot(host, spec, dry):
    """Schedules reboots in thread groups.

    Args: 
        host: The target host name
        spec: yaml definitions
        dry: Simulate or now

    Returns:
        None
    """
    # Not a dry run, just warn that its real
    if not dry:
        log("global", f"!!!!! NOT A DRY RUN !!!!!")
        log("global", f"!!!!! WILL REBOOT SERVERS !!!!!")
        log("global", f"!!!!! CTRL + C NOW TO ABORT !!!!!")
        time.sleep(5)
    else:
        log("global","DRY RUN")
        log("global","REBOOTS AND SAFE ACTIONS WILL NOT BE PERFORMED")
    # Get the kernel spec, otherwise set to 999 and force a reboot
    kernel = spec.get('kernel', "999")
    hostglob = host
    # Set signal callbacks
    signal.signal(signal.SIGINT, raise_graceful_exit)
    signal.signal(signal.SIGTERM, raise_graceful_exit)
    log("global","Gathering servers requiring kernel updates")
    target_type = spec['target_type']
    # Call kernel_check module to sort the hosts into groups and discover kernels
    c = kernel_check.check(kernel, hostglob, target_type)
    if len(c) <= 0:
        log("global","No reboots needed for this host group")
        return
    log("global","Obtained reboot groups")
    # Start a threadpool to run hosts by groups. Groups are hostname[:8] prefix
    with concurrent.futures.ThreadPoolExecutor() as ex:
        tasklist = []
        for k in c:
            if len(k) > 0:
                calling_group = list(k.keys())[0][:8]
                log("global", f"Starting group reboot {list(k.keys())[0][:8]}")
                log("global", f"Group members: {list(k.keys())}")
                tasklist.append(ex.submit(reboot_group, k, calling_group, spec, dry))
        for future in concurrent.futures.as_completed(tasklist):
            log("global", f"{future} thread completed {future.result()}")


# Argparse, load yaml, etc...
if __name__ == "__main__":
    dry = True
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--confirm',action='store_false', default=True, help='Actually do the reboots. Default is to do a test run of pre and post checks')
    parser.add_argument('--yaml',required=True,help='Path to the yaml reboot definition')
    args = parser.parse_args()
    if not args.confirm:
        dry = False
    config = loadYamlFromLocalDisk(args.yaml)
    if not config:
        log("global", "Could not load configuration file")
        log("global", "Check syntax and file path")
        exit(-1)
    for host in config.keys():
        kernel_tgt = config[host].get('kernel', False)
        if kernel_tgt:
            kernel = f"Target Kernel: {config[host]['kernel']}"
        else:
            kernel = "Forcing Reboots"
        log("global", f"Checking {host} for required reboots. {kernel}")
        spec = config[host]
        do_reboot(host, spec, dry)
        log("global", f"Group {host} complete")
