import paramiko
from dagster import ConfigurableResource


class SSHResource(ConfigurableResource):
    host: str
    username: str
    password: str

    def get_client(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.host, username=self.username, password=self.password)
        return ssh
