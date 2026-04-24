FROM mambaorg/micromamba:latest

WORKDIR /app

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yml /tmp/environment.yml
RUN micromamba create -y -n mqtt2postgres -f /tmp/environment.yml && \
    micromamba clean --all --yes

COPY --chown=$MAMBA_USER:$MAMBA_USER pyproject.toml README.md main.py /app/
COPY --chown=$MAMBA_USER:$MAMBA_USER contracts /app/contracts
COPY --chown=$MAMBA_USER:$MAMBA_USER src /app/src

ENV PATH=/opt/conda/envs/mqtt2postgres/bin:$PATH
ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "mqtt2postgres.cli"]
