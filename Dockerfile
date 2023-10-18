FROM python:3.12-slim

# Install prereqs and initialize python user
RUN apt update \
 && apt upgrade \
 && apt install time make \
 && adduser --system --no-create-home python \
 && mkdir /mnt/output \
 && chown python /mnt/output

# /mnt/output is where the current working directory should be mounted
WORKDIR /mnt/output

USER python
CMD ["bash", "run.sh"]
