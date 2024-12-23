import sys
import os
import cv2
import rclpy
from rclpy.node import Node
import csv
from cv_bridge import CvBridge
from sensor_msgs.msg import Imu, Image, CameraInfo
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Quaternion
import time


def extract_frames(video_path, output_dir):
    cap = cv2.VideoCapture(video_path)
    frame_count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        output_path = os.path.join(output_dir, f"{frame_count:06d}.jpg")
        cv2.imwrite(output_path, frame)
        frame_count += 1
    cap.release()


def read_csv(file_path):
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        data = [row for row in reader]
    return data


def combine_and_sort_data(imu_data, odometry_data, image_data):
    combined_data = []

    # Combine IMU data
    for row in imu_data[1:]:  # Skip header
        combined_data.append({"timestamp": float(row[0]), "type": "imu", "data": row})

    # Combine Odometry data
    for row in odometry_data[1:]:  # Skip header
        combined_data.append({"timestamp": float(row[0]), "type": "odometry", "data": row})

    # Combine Image data
    for image_info in image_data:
        combined_data.append({"timestamp": image_info["timestamp"], "type": "image", "data": image_info})

    # Sort by timestamp
    combined_data.sort(key=lambda x: x["timestamp"])
    return combined_data


class CSVAndVideoPublisher(Node):
    def __init__(self, data_dir):
        super().__init__("csv_and_video_publisher")

        # Publishers
        self.imu_pub = self.create_publisher(Imu, "/imu", 10)
        self.odometry_pub = self.create_publisher(Odometry, "/odometry", 10)
        self.image_pub = self.create_publisher(Image, "/camera/image", 10)
        self.camera_info_pub = self.create_publisher(CameraInfo, "/camera_info", 10)

        # Load CSV data
        imu_data = read_csv(f"{data_dir}/imu.csv")[1:]  # Skip header
        odometry_data = read_csv(f"{data_dir}/odometry.csv")[1:]  # Skip header

        # Extract video frames if necessary
        self.image_dir = os.path.join(data_dir, "images")
        if not os.path.exists(self.image_dir):
            os.makedirs(self.image_dir)
            extract_frames(os.path.join(data_dir, "rgb.mp4"), self.image_dir)

        # Prepare Image data with timestamps from odometry.csv
        image_data = []
        for row in odometry_data:
            frame_id = int(row[1])
            timestamp = float(row[0])
            image_path = os.path.join(self.image_dir, f"{frame_id:06d}.jpg")
            if os.path.exists(image_path):
                image_data.append({"timestamp": timestamp, "path": image_path})

        # Combine and sort all data
        self.sorted_data = combine_and_sort_data(imu_data, odometry_data, image_data)
        self.current_index = 0
        self.bridge = CvBridge()

        # Start publishing
        self.start_time = time.time()
        self.initial_timestamp = self.sorted_data[0]["timestamp"]
        self.timer = self.create_timer(0.0005, self.publish_data)  # High frequency timer

    def publish_data(self):
        if self.current_index >= len(self.sorted_data):
            self.get_logger().info("All data published.")
            self.destroy_timer(self.timer)
            return

        current_time = time.time()
        elapsed_time = current_time - self.start_time

        while (
            self.current_index < len(self.sorted_data)
            and self.sorted_data[self.current_index]["timestamp"] - self.initial_timestamp <= elapsed_time
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

            elif entry["type"] == "image":
                image_info = entry["data"]
                img = cv2.imread(image_info["path"])
                if img is not None:
                    img_msg = self.bridge.cv2_to_imgmsg(img, encoding="bgr8")
                    img_msg.header.stamp.sec = int(timestamp)
                    img_msg.header.stamp.nanosec = int((timestamp - int(timestamp)) * 1e9)
                    img_msg.header.frame_id = "camera_frame"
                    self.image_pub.publish(img_msg)

            self.current_index += 1


def main(args=None):
    rclpy.init(args=args)

    if len(sys.argv) < 2:
        print("Usage: python3 pub_combined_data.py <data_directory>")
        rclpy.shutdown()
        return

    data_dir = sys.argv[1]
    node = CSVAndVideoPublisher(data_dir)
    rclpy.spin(node)

    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
