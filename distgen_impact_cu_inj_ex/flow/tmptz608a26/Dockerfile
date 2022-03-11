
FROM condaforge/mambaforge AS build

# Create environment
COPY ./environment.yml .
RUN conda env create -f ./environment.yml

# Install conda-pack:
RUN conda install -c conda-forge conda-pack

# Use conda-pack to create a  enviornment in /venv:
RUN conda-pack -n distgen-impact-cu-inj-ex -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# Fix paths, will be same in final image so this is fine
RUN /venv/bin/conda-unpack

# No longer need conda, just the packed python
FROM debian:buster AS runtime

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

COPY _entrypoint.sh /usr/local/bin/_entrypoint.sh
RUN chmod +x /usr/local/bin/_entrypoint.sh

RUN mkdir /opt/prefect
COPY flow.py /opt/prefect/flow.py

# When image is run, run the code with the environment
# activated:
SHELL ["/usr/local/bin/_entrypoint.sh", "/bin/bash", "-c"]

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
ENV PREFECT__USER_CONFIG_PATH='/opt/prefect/config.toml'

RUN pip install pip --upgrade
RUN pip show prefect || pip install git+https://github.com/PrefectHQ/prefect.git@0.15.13#egg=prefect[all_orchestration_extras]

RUN pip install wheel 

RUN mkdir -p /opt/prefect/

COPY ./tmptz608a26/healthcheck.py /opt/prefect/healthcheck.py



RUN python /opt/prefect/healthcheck.py '["/opt/prefect/flow.py"]' '(3, 9)'
