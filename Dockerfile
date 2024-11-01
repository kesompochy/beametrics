FROM gcr.io/dataflow-templates-base/python312-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/beametrics/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/template/setup.py" 

COPY requirements.txt /template/
COPY beametrics/ /template/beametrics/
COPY setup.py /template/

RUN apt-get update \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /template/requirements.txt \
    && cd /template && pip install .

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
