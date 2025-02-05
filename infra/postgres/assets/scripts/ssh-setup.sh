#!/bin/bash
apt update && apt install openssh-server sudo -y
useradd -rm -d /home/ubuntu -s /bin/bash -g root -G sudo -u 1000 sshuser
echo "sshuser:sshuser" | chpasswd
cp -f /assets/config/sshd_config /etc/ssh/sshd_config 
chown -R sshuser /tmp/staging
