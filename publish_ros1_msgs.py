#!/usr/bin/env python

import sys
import struct
import os
import cv2
import rospy
import csv
import tf
from cv_bridge import CvBridge
from sensor_msgs.msg import Imu, Image
from sensor_msgs.msg import PointCloud2, PointField
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Quaternion
from geometry_msgs.msg import TransformStamped
import numpy as np
import time
from std_msgs.msg import Header
from tqdm import tqdm

def convert_video_to_images(video_path, output_dir):
    """
    Convert video file to image sequence.
    
    Args:
        video_path (str): Path to video file
        output_dir (str): Directory to save image sequence
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    with tqdm(total=total_frames, desc="Converting rgb.mp4 into separate images") as pbar:
        frame_count = 0
        
        while True:
            ret, frame = cap.read()
            if not ret:
                break
                
            output_path = os.path.join(output_dir, f"{frame_count:06d}.jpg")
            cv2.imwrite(output_path, frame)
            frame_count += 1
            pbar.update(1)
        
    cap.release()


def read_csv(file_path):
    """
    Read CSV file and return data as list of rows.
    
    Args:
        file_path (str): Path to CSV file
        
    Returns:
        list: List of rows from CSV file
    """
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        data = [row for row in reader]
    return data


def adjust_rgb_and_camera_matrix(rgb_image, depth_image, camera_matrix):
    """
    Adjust RGB image size to match Depth image and update camera matrix.

    Args:
        rgb_image (numpy.ndarray): Original RGB image
        depth_image (numpy.ndarray): Depth image
        camera_matrix (numpy.ndarray): Original camera intrinsic matrix

    Returns:
        tuple: (resized_rgb, adjusted_camera_matrix)
    """
    depth_h, depth_w = depth_image.shape[:2]
    rgb_h, rgb_w = rgb_image.shape[:2]

    scale_x = depth_w / rgb_w
    scale_y = depth_h / rgb_h

    resized_rgb = cv2.resize(rgb_image, (depth_w, depth_h), interpolation=cv2.INTER_LINEAR)

    adjusted_camera_matrix = camera_matrix.copy()
    adjusted_camera_matrix[0, 0] *= scale_x  # fx
    adjusted_camera_matrix[1, 1] *= scale_y  # fy
    adjusted_camera_matrix[0, 2] *= scale_x  # cx
    adjusted_camera_matrix[1, 2] *= scale_y  # cy

    return resized_rgb, adjusted_camera_matrix


def create_pointcloud2(points_with_rgb):
    """
    Create PointCloud2 message from points with RGB values.

    Args:
        points_with_rgb (numpy.ndarray): Nx6 array containing points and colors (x,y,z,r,g,b)

    Returns:
        PointCloud2: ROS PointCloud2 message
    """
    pointcloud_msg = PointCloud2()
    pointcloud_msg.header.frame_id = "camera_depth_frame"

    fields = [
        PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name="rgb", offset=12, datatype=PointField.UINT32, count=1),
    ]
    pointcloud_msg.fields = fields
    pointcloud_msg.point_step = 16
    pointcloud_msg.row_step = pointcloud_msg.point_step * len(points_with_rgb)

    buffer = []
    for point in points_with_rgb:
        x, y, z, r, g, b = point
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
        depth_image (numpy.ndarray): Depth image in mm (uint16)
        rgb_image (numpy.ndarray): RGB image matching the depth image size
        camera_matrix (numpy.ndarray): Camera intrinsic matrix

    Returns:
        numpy.ndarray: Nx6 array of 3D points with RGB colors (x, y, z, r, g, b)
    """
    fx, fy = camera_matrix[0, 0], camera_matrix[1, 1]
    cx, cy = camera_matrix[0, 2], camera_matrix[1, 2]

    depth_h, depth_w = depth_image.shape
    u, v = np.meshgrid(np.arange(depth_w), np.arange(depth_h))
    u = u.flatten()
    v = v.flatten()

    z = depth_image.flatten().astype(np.float32) / 1000.0  # Convert mm to meters
    valid = z > 0
    z = z[valid]
    u = u[valid]
    v = v[valid]

    x = (u - cx) * z / fx
    y = (v - cy) * z / fy
    points_3d = np.stack((x, y, z), axis=-1)

    rgb_flat = rgb_image.reshape(-1, 3)
    colors = rgb_flat[valid]

    point_cloud = np.concatenate((points_3d, colors), axis=-1)

    return point_cloud


def combine_and_sort_data(imu_data, odometry_data, rgb_data, depth_data):
    """
    Combine and sort all data by timestamp.

    Args:
        imu_data (list): List of IMU data rows
        odometry_data (list): List of odometry data rows
        rgb_data (list): List of RGB image data
        depth_data (list): List of depth image data

    Returns:
        list: Combined and sorted data by timestamp
    """
    combined_data = []

    for row in imu_data:
        combined_data.append({"timestamp": float(row[0]), "type": "imu", "data": row})

    for row in odometry_data:
        combined_data.append(
            {"timestamp": float(row[0]), "type": "odometry", "data": row}
        )

    for image_info in rgb_data:
        combined_data.append(
            {"timestamp": image_info["timestamp"], "type": "rgb", "data": image_info}
        )

    for image_info in depth_data:
        combined_data.append(
            {"timestamp": image_info["timestamp"], "type": "depth", "data": image_info}
        )

    combined_data.sort(key=lambda x: x["timestamp"])
    return combined_data


class StrayScannerDataPublisher:
    def __init__(self, data_dir):
        """
        Initialize ROS1 publisher node for Stray Scanner data.

        Args:
            data_dir (str): Directory containing scanner data
        """
        # Initialize publishers
        self.imu_pub = rospy.Publisher("/imu", Imu, queue_size=100)
        self.odometry_pub = rospy.Publisher("/odometry", Odometry, queue_size=100)
        self.rgb_pub = rospy.Publisher("/camera/rgb", Image, queue_size=100)
        self.depth_pub = rospy.Publisher("/camera/depth", Image, queue_size=100)
        self.pointcloud_pub = rospy.Publisher("/pointcloud", PointCloud2, queue_size=100)

        # Load data
        self.bridge = CvBridge()
        self.data_dir = data_dir
        self.load_data()
        
        # Initialize progress bar
        self.progress_bar = tqdm(total=len(self.sorted_data), desc="Publishing data")
        
        # Start publishing
        self.start_time = time.time()
        self.current_index = 0
        self.initial_timestamp = self.sorted_data[0]["timestamp"]
        
        # Start timer for publishing
        rospy.Timer(rospy.Duration(0.001), self.publish_data)
        
        # Add tf broadcaster
        self.tf_broadcaster = tf.TransformBroadcaster()

        # Define transform tree
        self.transforms = {
            'map': 'odom_frame',
            'odom_frame': 'base_link',
            'base_link': 'imu_frame',
            'base_link': 'camera_rgb_frame',
            'camera_rgb_frame': 'camera_depth_frame'
        }

    def prepare_image_data(self, image_dir, odometry_data):
        """
        Prepare image data from directory or convert video if necessary.

        Args:
            image_dir (str): Directory containing images or video
            odometry_data (list): List of odometry data rows

        Returns:
            list: List of image data with timestamps
        """
        image_data = []
        
        # Check if image directory exists
        if not os.path.exists(image_dir):
            os.makedirs(image_dir)
        
        # Check for video file if directory is empty and it's RGB data
        video_path = os.path.join(self.data_dir, "rgb.mp4")
        if "depth" not in image_dir and len(os.listdir(image_dir)) == 0 and os.path.exists(video_path):
            rospy.loginfo(f"Converting video to images: {video_path}")
            convert_video_to_images(video_path, image_dir)
        
        # After potential video conversion, check if we have images
        if len(os.listdir(image_dir)) == 0:
            rospy.logerr(f"No images or video found in directory: {image_dir}")
            return image_data
        
        # Process image data
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

    def load_data(self):
        """Load and prepare all data from files"""
        # Load CSV data
        imu_data = read_csv(os.path.join(self.data_dir, "imu.csv"))[1:]
        odometry_data = read_csv(os.path.join(self.data_dir, "odometry.csv"))[1:]

        # Prepare image data
        self.rgb_dir = os.path.join(self.data_dir, "images")
        rgb_data = self.prepare_image_data(self.rgb_dir, odometry_data)

        self.depth_dir = os.path.join(self.data_dir, "depth")
        depth_data = self.prepare_image_data(self.depth_dir, odometry_data)

        # Load camera matrix
        self.rgb_intrinsic_matrix = np.array(
            read_csv(os.path.join(self.data_dir, "camera_matrix.csv")), 
            dtype=float
        )

        # Combine and sort all data
        self.sorted_data = combine_and_sort_data(
            imu_data, odometry_data, rgb_data, depth_data
        )

    def publish_data(self, event=None):
        """
        Timer callback to publish data at appropriate timestamps.
        """
        if self.current_index >= len(self.sorted_data):
            self.progress_bar.close()
            rospy.loginfo("All data published; Shutting down and removing transforms.")
            rospy.signal_shutdown()
            self.tf_broadcaster.sendTransform((0, 0, 0), (0, 0, 0, 1), rospy.Time.now(), "base_link", "odom_frame")
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
            ros_time = rospy.Time.from_sec(timestamp)

            if entry["type"] == "imu":
                imu_row = entry["data"]
                imu_msg = Imu()
                imu_msg.header.stamp = ros_time
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
                odom_msg.header.stamp = ros_time
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

                # Publish transform from odom to base_link
                self.tf_broadcaster.sendTransform(
                    (float(odom_row[2]), float(odom_row[3]), float(odom_row[4])),  # translation
                    (float(odom_row[5]), float(odom_row[6]), float(odom_row[7]), float(odom_row[8])),  # rotation
                    ros_time,
                    "base_link",
                    "odom_frame"
                )

                # Publish static transforms
                self.publish_static_transforms(ros_time)

            elif entry["type"] == "rgb":
                image_info = entry["data"]
                bgr_img = cv2.imread(image_info["path"])
                if bgr_img is not None:
                    rgb_img = cv2.cvtColor(bgr_img, cv2.COLOR_BGR2RGB)
                    img_msg = self.bridge.cv2_to_imgmsg(bgr_img, encoding="bgr8")
                    img_msg.header.stamp = ros_time
                    img_msg.header.frame_id = "camera_rgb_frame"
                    self.rgb_pub.publish(img_msg)

            elif entry["type"] == "depth":
                image_info = entry["data"]
                depth_img = cv2.imread(image_info["path"], cv2.IMREAD_UNCHANGED)
                if depth_img is not None and depth_img.dtype == np.uint16:
                    depth_msg = self.bridge.cv2_to_imgmsg(depth_img, encoding="mono16")
                    depth_msg.header.stamp = ros_time
                    depth_msg.header.frame_id = "camera_depth_frame"
                    self.depth_pub.publish(depth_msg)

                    resized_rgb, adjusted_camera_matrix = adjust_rgb_and_camera_matrix(
                        rgb_img, depth_img, self.rgb_intrinsic_matrix
                    )
                    points = unproject_depth(depth_img, resized_rgb, adjusted_camera_matrix)
                    pointcloud_msg = create_pointcloud2(points)
                    pointcloud_msg.header.stamp = ros_time
                    self.pointcloud_pub.publish(pointcloud_msg)

            self.progress_bar.update(1)
            self.current_index += 1


    def publish_static_transforms(self, timestamp):
        """
        Publish static transforms between frames.
        """
        # Camera RGB to base_link transform
        self.tf_broadcaster.sendTransform(
            (0.0, 0.0, 0.0),  # You might want to adjust these values
            (0.0, 0.0, 0.0, 1.0),  # Quaternion for no rotation
            timestamp,
            "camera_rgb_frame",
            "base_link"
        )

        # Camera depth to camera RGB transform
        self.tf_broadcaster.sendTransform(
            (0.0, 0.0, 0.0),  # Adjust based on your camera setup
            (0.0, 0.0, 0.0, 1.0),
            timestamp,
            "camera_depth_frame",
            "camera_rgb_frame"
        )

        # IMU to base_link transform
        self.tf_broadcaster.sendTransform(
            (0.0, 0.0, 0.0),  # Adjust based on your IMU position
            (0.0, 0.0, 0.0, 1.0),
            timestamp,
            "imu_frame",
            "base_link"
        )

        # Map to odom transform (identity transform)
        self.tf_broadcaster.sendTransform(
            (0.0, 0.0, 0.0),
            (0.0, 0.0, 0.0, 1.0),
            timestamp,
            "odom_frame",
            "map"
        )


def main():
    rospy.init_node("stray_scanner_publisher")

    if len(sys.argv) < 2:
        rospy.logerr("Usage: python publish_ros1_msgs.py <data_directory>")
        return

    data_dir = sys.argv[1]
    publisher = StrayScannerDataPublisher(data_dir)
    
    try:
        rospy.spin()
    except KeyboardInterrupt:
        publisher.progress_bar.close()
        rospy.loginfo("Shutting down")


if __name__ == "__main__":
    main()