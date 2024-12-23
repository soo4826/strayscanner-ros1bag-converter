# Set working directory
WORKDIR="$(pwd)"

# Set Docker image
IMAGE="ros1-noetic"

# Set container name
CONTAINER_NAME="ros1-container"

# Run Docker container (with GPU support and custom name)
xhost +local:root
docker run --rm -it \
    --gpus all \
    --name $CONTAINER_NAME \
    -v /tmp/.X11-unix:/tmp/.X11-unix \
    -e DISPLAY=$DISPLAY \
    -v "$WORKDIR":/ws \
    -w /ws \
    -p 8888:8888 \
    $IMAGE \
    /bin/bash