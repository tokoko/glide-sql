FROM python:3.10
RUN apt-get update && apt-get install -y curl unzip zip make git graphviz
RUN curl -s "https://get.sdkman.io" | bash
SHELL ["/bin/bash", "-c"]

# RUN source "/root/.sdkman/bin/sdkman-init.sh"   \
#                 && sdk install java 11.0.26-tem \
#                 && sdk install maven 3.9.9
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y