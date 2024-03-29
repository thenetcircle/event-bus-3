ARG REPO_LOCATION=thin.thenetcircle.lab:5000/dockerhub-replica/
ARG VERSION=3.9.16-slim
ARG PORT

# can be overwritten locally to not use Harbor by adding "--build-arg":
# docker build -t quiztest --build-arg REPO_LOCATION="" .
FROM ${REPO_LOCATION}python:${VERSION}

WORKDIR /app

COPY requirements.txt ./

RUN pip3 install --no-cache-dir gunicorn uvicorn

RUN pip3 install \
         --no-cache-dir \
         -i http://fat.thenetcircle.lab:5005/index/ \
         --trusted-host fat.thenetcircle.lab \
         -r requirements.txt

ENV PYTHONPATH .

COPY eventbus eventbus
COPY tests tests
COPY configs configs
COPY pyproject.toml ./
COPY entrypoint.sh ./

RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "/app/entrypoint.sh" ]
