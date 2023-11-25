###############################################################################
# This dockerfile creates an Buildkite agent image.
# Run the following command to build and run the Buildkite agent 
# in the container:
# $docker build -t buildkite-agent .
# $docker run -e BUILDKITE_AGENT_TOKEN=$BUILDKITE_AGENT_TOKEN 
# --name buildkite-agent buildkite-agent
# Please ensure the environmental variable "BUILDKITE_AGENT_TOKEN" is defined
# prior to running the commands.
###############################################################################
FROM python:3.8-slim-bookworm

# Install curl and other dependencies
RUN apt-get update && \
    apt-get install -y \
    curl \
    bash \
    git \
    make \
    automake \
    gcc \
    g++ \
    subversion \
    python3-dev

# Download and run the install script for Buildkite Agent
RUN bash -c "`curl -sL https://raw.githubusercontent.com/buildkite/agent/master/install.sh`"

# Set the command to start the buildkite agent
CMD /root/.buildkite-agent/bin/buildkite-agent start