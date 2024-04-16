#!/bin/bash

PROD_K8S_SERVER="https://10.20.4.200:6443"
PROD_SERVICETEAM_USER="sa-serviceteam"


echo -n "Please input token of production k8s serviceteam serviceaccount: "
read PROD_SERVICETEAM_TOKEN
if [ -z "$PROD_SERVICETEAM_TOKEN" ] ; then
    echo "You should input the token!"
    exit 1
fi

/usr/bin/kubectl config set-cluster tnc-k8s-prod --server="${PROD_K8S_SERVER}" --insecure-skip-tls-verify=true
/usr/bin/kubectl config set-credentials ${PROD_SERVICETEAM_USER} --token="${PROD_SERVICETEAM_TOKEN}"
/usr/bin/kubectl config set-context prod-serviceteam-context --cluster=tnc-k8s-prod --user=${PROD_SERVICETEAM_USER}
/usr/bin/kubectl config use-context prod-serviceteam-context
