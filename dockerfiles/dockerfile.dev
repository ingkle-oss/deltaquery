FROM rust:latest

# Prepare Timezone setting
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y git sudo

# Setup a user(developer) account
ARG UID=1000
ARG GID=1000

RUN addgroup --gid ${GID} developer
RUN useradd -s /bin/bash -d /home/developer -u ${UID} -g ${GID} developer
RUN mkdir -p /home/developer
RUN chown developer:developer /home/developer

RUN echo 'developer ALL=(ALL) NOPASSWD:ALL' | tee -a /etc/sudoers.d/developer
RUN chmod -w /etc/sudoers.d/developer

USER developer
WORKDIR /home/developer

CMD /bin/bash
