import paramiko
import time

#Setup base connection, put it in a function probably
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('69.164.202.83', username='root', key_filename='./.ssh/id_rsa')
connection = ssh.invoke_shell()

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

data = send_command(connection, ["ls /etc","ls /root"])
print(data)
