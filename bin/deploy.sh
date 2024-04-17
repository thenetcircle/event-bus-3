#!/bin/bash


# Default values for optional arguments
target="both"
version="latest"

# Assign arguments to variables with default values for optional ones
community=$1
environment=$2
[ -n "$3" ] && target=$3
[ -n "$4" ] && version=$4

# Check if the community is valid
if [[ "$community" != "popp" && "$community" != "gays" && "$community" != "feti" && "$community" != "kauf" && "$community" != "happ" ]]; then
  echo "Invalid community: $community"
  echo "Usage: $0 [community] [environment] [target] [version]"
  exit 1
fi

# Check if the environment is valid
if [[ "$environment" != "staging" && "$environment" != "prod" ]]; then
  echo "Invalid environment: $environment"
  echo "Usage: $0 [community] [environment] [target] [version]"
  exit 1
fi

# Check if the target is valid
if [[ "$target" != "producer" && "$target" != "consumer" && "$target" != "both" ]]; then
  echo "Invalid target: $target"
  echo "Usage: $0 [community] [environment] [target] [version]"
  exit 1
fi

current_dir=$(dirname "$0")
# Define the remote server parameters
remote_username="serviceteam"
remote_host="kf-deploy" #kf-deploy.thenetcircle.lab

deploy() {
  component=$1
  echo "Deploying $component on $environment environment with version $version"

  # Define the directory to copy and modify
  src_directory="${current_dir}/../deployment/charts/${component}-${environment}"
  dst_directory=$(mktemp -d)

  # Copy the directory
  cp -r $src_directory/ $dst_directory

  # Replace some text in the directory (modify this to your need)
  if [[ "$(uname)" == "Darwin" ]]; then
    # macOS specific commands
    find $dst_directory -type f -exec sed -i '' "s/{comm}/${community}/g" {} +
    find $dst_directory -type f -exec sed -i '' "s/{env}/${environment}/g" {} +
    sed -i '' -E "s#appVersion: \"[^\"]+\"#appVersion: \"${version}\"#" $dst_directory/Chart.yaml
  else
    # Linux specific commands
    find $dst_directory -type f -exec sed -i "s/{comm}/${community}/g" {} +
    find $dst_directory -type f -exec sed -i "s/{env}/${environment}/g" {} +
    sed -i -E "s#appVersion: \"[^\"]+\"#appVersion: \"${version}\"#" $dst_directory/Chart.yaml
  fi

  remote_directory="/home/serviceteam/eventbus3/deployment/${community}_${environment}/${component}"

  ssh $remote_username@$remote_host "mkdir -p $remote_directory; rm -rf $remote_directory/*"

  # SCP the directory to the remote server
  scp -r $dst_directory/* $remote_username@$remote_host:$remote_directory

  chart_name="eventbus-${component}-${community}"
  [ "$environment" == "staging" ] && chart_name="${chart_name}-staging"

  current_time=$(date +%s)
  remote_command="helm -n serviceteam upgrade --install --set image.tag=$version --set podAnnotations.restartDate=\"\\\"${current_time}\\\"\" $chart_name $remote_directory"
  echo "Running command: $remote_command"
  ssh $remote_username@$remote_host $remote_command

  # echo "Remove directory: $dst_directory"
  rm -rf "$dst_directory"
}

# Check what to deploy and run the corresponding helm commands
if [ "$target" == "producer" ] || [ "$target" == "both" ]; then
  deploy producer
fi

if [ "$target" == "consumer" ] || [ "$target" == "both" ]; then
  deploy consumer
fi

echo "Deployment completed successfully."
