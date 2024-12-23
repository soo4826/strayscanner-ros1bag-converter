import sys
import struct
import os
import cv2
import rclpy
from rclpy.node import Node
import csv
from cv_bridge import CvBridge
from sensor_msgs.msg import Imu, Image
from sensor_msgs.msg import PointCloud2, PointField
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Quaternion
import numpy as np
import time
from std_msgs.msg import Header
from tqdm import tqdm 


def adjust_rgb_and_camera_matrix(rgb_image, depth_image, camera_matrix):
    """
    Adjust RGB image size to match Depth image and update camera matrix.

    Args:
        rgb_image (numpy.ndarray): Original RGB image.
        depth_image (numpy.ndarray): Depth image.
        camera_matrix (numpy.ndarray): Original camera intrinsic matrix.

    Returns:
        resized_rgb (numpy.ndarray): Resized RGB image.
        adjusted_camera_matrix (numpy.ndarray): Adjusted camera intrinsic matrix.
    """

    depth_h, depth_w = depth_image.shape[:2]
    rgb_h, rgb_w = rgb_image.shape[:2]

    # Calculate scale factors
    scale_x = depth_w / rgb_w
    scale_y = depth_h / rgb_h

    # Resize RGB image
    resized_rgb = cv2.resize(
        rgb_image, (depth_w, depth_h), interpolation=cv2.INTER_LINEAR
    )

    # Adjust camera matrix
    adjusted_camera_matrix = camera_matrix.copy()
    adjusted_camera_matrix[0, 0] *= scale_x  # fx
    adjusted_camera_matrix[1, 1] *= scale_y  # fy
    adjusted_camera_matrix[0, 2] *= scale_x  # cx
    adjusted_camera_matrix[1, 2] *= scale_y  # cy

    return resized_rgb, adjusted_camera_matrix


def create_pointcloud2(points_with_rgb):
    """
    Create a PointCloud2 message from 3D points with RGB.

    Args:
        points_with_rgb (numpy.ndarray): Nx6 array of 3D points with RGB (x, y, z, r, g, b).

    Returns:
        pointcloud_msg (PointCloud2): ROS 2 PointCloud2 message.
    """
    pointcloud_msg = PointCloud2()

    # Header
    pointcloud_msg.header.frame_id = "camera_depth_frame"

    # Define fields for PointCloud2
    fields = [
        PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name="rgb", offset=12, datatype=PointField.UINT32, count=1),
    ]
    pointcloud_msg.fields = fields

    # Calculate point step and row step
    pointcloud_msg.point_step = 16  # 4 bytes (float32) * 3 + 4 bytes (uint32)
    pointcloud_msg.row_step = pointcloud_msg.point_step * len(points_with_rgb)

    # Convert points to byte data
    buffer = []
    for point in points_with_rgb:
        x, y, z, r, g, b = point
        # Ensure RGB values are in 0-255 range and convert to uint32
        r, g, b = int(r), int(g), int(b)
        rgb = struct.unpack("I", struct.pack("BBBB", b, g, r, 0))[0]
        buffer.append(struct.pack("fffI", x, y, z, rgb))

    pointcloud_msg.data = b"".join(buffer)
    pointcloud_msg.is_bigendian = False
    pointcloud_msg.height = 1
    pointcloud_msg.width = len(points_with_rgb)
    pointcloud_msg.is_dense = True

    return pointcloud_msg


def unproject_depth(depth_image, rgb_image, camera_matrix):
    """
    Generate a 3D point cloud with color from a depth image and camera matrix.

    Args:
        depth_image (numpy.ndarray): Depth image in mm (uint16).
        rgb_image (numpy.ndarray): RGB image matching the depth image size.
        camera_matrix (numpy.ndarray): Camera intrinsic matrix.

    Returns:
        point_cloud (numpy.ndarray): Nx6 array of 3D points with RGB colors (x, y, z, r, g, b).
    """
    # Get camera intrinsics
    fx, fy = camera_matrix[0, 0], camera_matrix[1, 1]
    cx, cy = camera_matrix[0, 2], camera_matrix[1, 2]

    # Create a grid of pixel coordinates
    depth_h, depth_w = depth_image.shape
    u, v = np.meshgrid(np.arange(depth_w), np.arange(depth_h))
    u = u.flatten()
    v = v.flatten()

    # Flatten depth image and filter valid depths
    z = depth_image.flatten().astype(np.float32) / 1000.0  # Convert mm to meters
    valid = z > 0  # Ignore invalid depth values
    z = z[valid]
    u = u[valid]
    v = v[valid]

    # Backproject to 3D
    x = (u - cx) * z / fx
    y = (v - cy) * z / fy
    points_3d = np.stack((x, y, z), axis=-1)  # Shape: (N, 3)

    # Add color from RGB image
    rgb_flat = rgb_image.reshape(-1, 3)  # Flatten RGB image
    colors = rgb_flat[valid]  # Get colors for valid depth points

    # Combine points and colors
    point_cloud = np.concatenate((points_3d, colors), axis=-1)  # Shape: (N, 6)

    return point_cloud


def read_csv(file_path):
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        data = [row for row in reader]
    return data


def combine_and_sort_data(imu_data, odometry_data, rgb_data, depth_data):
    combined_data = []

    # Combine IMU data
    for row in imu_data[1:]:  # Skip header
        combined_data.append({"timestamp": float(row[0]), "type": "imu", "data": row})

    # Combine Odometry data
    for row in odometry_data[1:]:  # Skip header
        combined_data.append(
            {"timestamp": float(row[0]), "type": "odometry", "data": row}
        )

    # Combine RGB data
    for image_info in rgb_data:
        combined_data.append(
            {"timestamp": image_info["timestamp"], "type": "rgb", "data": image_info}
        )

    # Combine Depth data
    for image_info in depth_data:
        combined_data.append(
            {"timestamp": image_info["timestamp"], "type": "depth", "data": image_info}
        )

    # Sort by timestamp
    combined_data.sort(key=lambda x: x["timestamp"])
    return combined_data


class StrayScannerDataPublisher(Node):
    def __init__(self, data_dir):
        super().__init__("csv_and_video_publisher")

        # Publishers
        self.imu_pub = self.create_publisher(Imu, "/imu", 100)
        self.odometry_pub = self.create_publisher(Odometry, "/odometry", 100)
        self.rgb_pub = self.create_publisher(Image, "/camera/rgb", 100)
        self.depth_pub = self.create_publisher(Image, "/camera/depth", 100)
        self.pointcloud_pub = self.create_publisher(PointCloud2, "/pointcloud", 100)

        # Load CSV data
        imu_data = read_csv(f"{data_dir}/imu.csv")[1:]  # Skip header
        odometry_data = read_csv(f"{data_dir}/odometry.csv")[1:]  # Skip header

        # Prepare RGB data
        self.rgb_dir = os.path.join(data_dir, "images")
        rgb_data = self.prepare_image_data(self.rgb_dir, odometry_data)
        self.num_frames = len(rgb_data)

        # Prepare Depth data
        self.depth_dir = os.path.join(data_dir, "depth")
        depth_data = self.prepare_image_data(self.depth_dir, odometry_data)

        # Prepare intrinsic matrix
        self.rgb_intrinsic_matrix = np.array(
            read_csv(f"{data_dir}/camera_matrix.csv"), dtype=float
        )

        # Combine and sort all data
        self.sorted_data = combine_and_sort_data(
            imu_data, odometry_data, rgb_data, depth_data
        )
        self.current_index = 0
        self.bridge = CvBridge()

        # Initialize progress bar
        self.progress_bar = tqdm(total=len(self.sorted_data), desc="Publishing data")

        # Start publishing
        self.start_time = time.time()
        self.initial_timestamp = self.sorted_data[0]["timestamp"]
        self.timer = self.create_timer(0.001, self.publish_data)  # High frequency timer

    def prepare_image_data(self, image_dir, odometry_data):
        image_data = []
        for row in odometry_data:
            frame_id = int(row[1])
            timestamp = float(row[0])
            image_path = os.path.join(
                image_dir,
                (
                    f"{frame_id:06d}.png"
                    if "depth" in image_dir
                    else f"{frame_id:06d}.jpg"
                ),
            )
            if os.path.exists(image_path):
                image_data.append({"timestamp": timestamp, "path": image_path})
        return image_data

    def publish_data(self):

        if self.current_index >= len(self.sorted_data):
            self.progress_bar.close()  # Close progress bar
            self.get_logger().info("All data published.")
            self.destroy_timer(self.timer)
            return

        current_time = time.time()
        elapsed_time = current_time - self.start_time

        while (
            self.current_index < len(self.sorted_data)
            and self.sorted_data[self.current_index]["timestamp"]
            - self.initial_timestamp
            <= elapsed_time
        ):
            entry = self.sorted_data[self.current_index]
            timestamp = entry["timestamp"]

            if entry["type"] == "imu":
                imu_row = entry["data"]
                imu_msg = Imu()
                imu_msg.header.stamp.sec = int(timestamp)
                imu_msg.header.stamp.nanosec = int((timestamp - int(timestamp)) * 1e9)
                imu_msg.header.frame_id = "imu_frame"
                imu_msg.linear_acceleration.x = float(imu_row[1])
                imu_msg.linear_acceleration.y = float(imu_row[2])
                imu_msg.linear_acceleration.z = float(imu_row[3])
                imu_msg.angular_velocity.x = float(imu_row[4])
                imu_msg.angular_velocity.y = float(imu_row[5])
                imu_msg.angular_velocity.z = float(imu_row[6])
                self.imu_pub.publish(imu_msg)

            elif entry["type"] == "odometry":
                odom_row = entry["data"]
                odom_msg = Odometry()
                odom_msg.header.stamp.sec = int(timestamp)
                odom_msg.header.stamp.nanosec = int((timestamp - int(timestamp)) * 1e9)
                odom_msg.header.frame_id = "odom_frame"
                odom_msg.child_frame_id = "base_link"
                odom_msg.pose.pose.position.x = float(odom_row[2])
                odom_msg.pose.pose.position.y = float(odom_row[3])
                odom_msg.pose.pose.position.z = float(odom_row[4])
                odom_msg.pose.pose.orientation = Quaternion(
                    x=float(odom_row[5]),
                    y=float(odom_row[6]),
                    z=float(odom_row[7]),
                    w=float(odom_row[8]),
                )
                self.odometry_pub.publish(odom_msg)

            elif entry["type"] == "rgb":
                image_info = entry["data"]
                bgr_img = cv2.imread(image_info["path"])
                rgb_img = cv2.cvtColor(bgr_img, cv2.COLOR_BGR2RGB)
                if rgb_img is not None:
                    img_msg = self.bridge.cv2_to_imgmsg(bgr_img, encoding="bgr8")
                    img_msg.header.stamp.sec = int(timestamp)
                    img_msg.header.stamp.nanosec = int(
                        (timestamp - int(timestamp)) * 1e9
                    )
                    img_msg.header.frame_id = "camera_rgb_frame"
                    self.rgb_pub.publish(img_msg)

            elif entry["type"] == "depth":
                image_info = entry["data"]
                depth_img = cv2.imread(image_info["path"], cv2.IMREAD_UNCHANGED)
                if depth_img is not None and depth_img.dtype == np.uint16:
                    # pub depth image
                    depth_msg = self.bridge.cv2_to_imgmsg(depth_img, encoding="mono16")
                    depth_msg.header.stamp.sec = int(timestamp)
                    depth_msg.header.stamp.nanosec = int(
                        (timestamp - int(timestamp)) * 1e9
                    )
                    depth_msg.header.frame_id = "camera_depth_frame"
                    self.depth_pub.publish(depth_msg)

                    # Generate 3D point cloud
                    resized_rgb, adjusted_camera_matrix = adjust_rgb_and_camera_matrix(
                        rgb_img, depth_img, self.rgb_intrinsic_matrix
                    )
                    points = unproject_depth(
                        depth_img, resized_rgb, adjusted_camera_matrix
                    )

                    # Convert points to PointCloud2 message
                    pointcloud_msg = create_pointcloud2(points)
                    pointcloud_msg.header.stamp = (
                        depth_msg.header.stamp
                    )  # Sync with depth image
                    self.pointcloud_pub.publish(pointcloud_msg)

            # Update progress bar
            self.progress_bar.update(1)

            self.current_index += 1


def main(args=None):
    rclpy.init(args=args)

    if len(sys.argv) < 2:
        print("Usage: python3 publish_ros2_msgs.py <data_directory>")
        rclpy.shutdown()
        return

    data_dir = sys.argv[1]
    node = StrayScannerDataPublisher(data_dir)
    rclpy.spin(node)

    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
