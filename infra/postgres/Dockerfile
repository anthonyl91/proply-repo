FROM ubuntu:22.04

ADD ./assets /assets

RUN mkdir /tmp/staging
# Enable ssh
RUN /assets/scripts/ssh-setup.sh
EXPOSE 22
#CMD ["/usr/sbin/sshd","-D"]

# Run postgres-setup
RUN /assets/scripts/postgres-setup.sh

# Run aws-cli setup
RUN /assets/scripts/aws-cli-setup.sh

CMD ["./assets/scripts/start-services.sh"]
