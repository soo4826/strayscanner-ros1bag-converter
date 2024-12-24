import sys
import os
import rospy
import rosbag
from sensor_msgs.msg import Imu, Image, PointCloud2
from nav_msgs.msg import Odometry
from tqdm import tqdm
from cv_bridge import CvBridge
import cv2
import numpy as np
from geometry_msgs.msg import Quaternion
import tf
import csv
import time

class StrayScannerRosbagConverter:
    def __init__(self, data_dir):
        """
        Initialize rosbag converter for Stray Scanner data.

        Args:
            data_dir (str): Directory containing scanner data
        """
        self.data_dir = data_dir
        self.bridge = CvBridge()

        # Prepare rosbag filename
        dir_name = os.path.basename(os.path.normpath(data_dir))
        bag_name = dir_name + ".bag"
        parent_dir = os.path.dirname(data_dir)
        self.bag_path = os.path.join(parent_dir, bag_name)
        self.bag = rosbag.Bag(self.bag_path, 'w')

        # Load data
        self.load_data()

        # Initialize progress bar
        self.progress_bar = tqdm(total=len(self.sorted_data), desc="Saving to rosbag")

        # Initialize time tracking
        self.start_time = time.time()
        self.current_index = 0
        self.initial_timestamp = self.sorted_data[0]["timestamp"]

        print(f"Rosbag will be saved to: {self.bag_path}")

    def read_csv(self, file_path):
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

        if not os.path.exists(image_dir):
            os.makedirs(image_dir)

        video_path = os.path.join(self.data_dir, "rgb.mp4")
        if "depth" not in image_dir and len(os.listdir(image_dir)) == 0 and os.path.exists(video_path):
            self.convert_video_to_images(video_path, image_dir)

        for row in odometry_data:
            frame_id = int(row[1])
            timestamp = float(row[0])
            image_path = os.path.join(
                image_dir,
                f"{frame_id:06d}.jpg",
            )
            if os.path.exists(image_path):
                image_data.append({"timestamp": timestamp, "path": image_path})

        return image_data

    def convert_video_to_images(self, video_path, output_dir):
        """
        Convert video file to image sequence.

        Args:
            video_path (str): Path to video file
            output_dir (str): Directory to save image sequence
        """
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        with tqdm(total=total_frames, desc="Converting video") as pbar:
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

    def load_data(self):
        """
        Load and prepare all data from files.
        """
        imu_data = self.read_csv(os.path.join(self.data_dir, "imu.csv"))[1:]
        odometry_data = self.read_csv(os.path.join(self.data_dir, "odometry.csv"))[1:]

        rgb_dir = os.path.join(self.data_dir, "images")
        rgb_data = self.prepare_image_data(rgb_dir, odometry_data)

        depth_dir = os.path.join(self.data_dir, "depth")
        depth_data = self.prepare_image_data(depth_dir, odometry_data)

        self.sorted_data = self.combine_and_sort_data(imu_data, odometry_data, rgb_data, depth_data)

    def combine_and_sort_data(self, imu_data, odometry_data, rgb_data, depth_data):
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
            combined_data.append({"timestamp": float(row[0]), "type": "odometry", "data": row})

        for image_info in rgb_data:
            combined_data.append({"timestamp": image_info["timestamp"], "type": "rgb", "data": image_info})

        for image_info in depth_data:
            combined_data.append({"timestamp": image_info["timestamp"], "type": "depth", "data": image_info})

        combined_data.sort(key=lambda x: x["timestamp"])
        return combined_data

    def write_to_rosbag(self):
        """
        Write all data to rosbag.
        """
        for entry in self.sorted_data:
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
                self.bag.write("/imu", imu_msg, t=ros_time)

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
                self.bag.write("/odometry", odom_msg, t=ros_time)

            elif entry["type"] == "rgb":
                image_info = entry["data"]
                bgr_img = cv2.imread(image_info["path"])
                if bgr_img is not None:
                    rgb_msg = self.bridge.cv2_to_compressed_imgmsg(bgr_img)
                    rgb_msg.header.stamp = ros_time
                    rgb_msg.header.frame_id = "camera_rgb_frame"
                    self.bag.write("/camera/rgb/compressed", rgb_msg, t=ros_time)

            elif entry["type"] == "depth":
                image_info = entry["data"]
                depth_img = cv2.imread(image_info["path"], cv2.IMREAD_UNCHANGED)
                if depth_img is not None and depth_img.dtype == np.uint16:
                    depth_msg = self.bridge.cv2_to_imgmsg(depth_img, encoding="mono16")
                    depth_msg.header.stamp = ros_time
                    depth_msg.header.frame_id = "camera_depth_frame"
                    self.bag.write("/camera/depth", depth_msg, t=ros_time)

            self.progress_bar.update(1)

        self.bag.close()
        print(f"Rosbag saved to {self.bag_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 convert_ros1_bag.py <data_directory>")
        return

    data_dir = sys.argv[1]
    if not os.path.exists(data_dir):
        print(f"Data directory {data_dir} does not exist.")
        return

    converter = StrayScannerRosbagConverter(data_dir)
    converter.write_to_rosbag()

if __name__ == "__main__":
    main()
