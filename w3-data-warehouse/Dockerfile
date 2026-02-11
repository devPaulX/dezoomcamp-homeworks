FROM google/cloud-sdk:latest

# Install pip if not already present
RUN apt-get update && apt-get install -y python3-pip

# Install required Python libraries (override Debian restriction)
RUN pip3 install --break-system-packages google-cloud-storage pandas pyarrow

WORKDIR /app
COPY . /app

CMD ["tail", "-f", "/dev/null"]
