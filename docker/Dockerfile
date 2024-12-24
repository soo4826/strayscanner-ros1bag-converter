FROM ros:noetic-ros-core-focal

# install bootstrap tools
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    python3-rosdep \
    python3-rosinstall \
    python3-vcstools \
    python3-pip \
    zip \
    && rm -rf /var/lib/apt/lists/*

# install python package
RUN pip3 install opencv-python tqdm

# bootstrap rosdep
RUN rosdep init && \
  rosdep update --rosdistro $ROS_DISTRO

# install ros desktop
RUN apt-get update && apt-get install -y --no-install-recommends \
    ros-noetic-desktop \
    ros-noetic-image-transport-plugins \
    && rm -rf /var/lib/apt/lists/*


RUN echo "source /opt/ros/noetic/setup.bash" >> /root/.bashrc
