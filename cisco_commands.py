import paramiko
import time


# Send commands, takes an ssh.invoke_shell object and
# a list of commands to be run. \r\n is sent after each command
# Returns full buffer of returned data
def send_command(connection, commands):
    for command in commands:
        connection.send(command)
        connection.send('\r\n')
    while not connection.recv_ready():
        time.sleep(1)
    lbuff = ""
    while connection.recv_ready():
        buff = connection.recv(65536).decode('utf-8')
        lbuff += buff
    return lbuff


if __name__ == "__main__":
    #Setup base connection, put it in a function probably
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('69.164.202.83', username='root', key_filename='./.ssh/id_rsa')
        connection = ssh.invoke_shell()
        data = send_command(connection, ["cd /etc", "ls", 'cd /tmp', "touch made_from_python"])
        print(data)
