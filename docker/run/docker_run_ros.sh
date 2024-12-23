# 작업 디렉토리 설정
WORKDIR="$(pwd)/../../"

# Docker 이미지 설정
IMAGE="ros:rolling-ros-core"

# 컨테이너 이름 설정
CONTAINER_NAME="ros-container"

# Docker 컨테이너 실행 (GPU 지원 및 이름 지정)
docker run --rm -it \
    --gpus all \
    --name "$CONTAINER_NAME" \
    -v "$WORKDIR":/ws \
    -w /ws \
    -p 8888:8888 \
    "$IMAGE" \
    /bin/bash

