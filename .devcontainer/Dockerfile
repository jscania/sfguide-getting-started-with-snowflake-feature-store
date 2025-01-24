FROM mcr.microsoft.com/devcontainers/miniconda:0-3

USER vscode
WORKDIR /home/vscode

# Create the conda environment
COPY environment.yml .
RUN conda env create \
    && conda init \
    && rm environment.yml
