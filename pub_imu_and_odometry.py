import sys 
import rclpy
from rclpy.node import Node
import csv
from sensor_msgs.msg import Imu, CameraInfo
from nav_msgs.msg import Odometry
from geometry_msgs.msg import Quaternion
import time


def read_csv(file_path):
    with open(file_path, "r") as f:
        reader = csv.reader(f)
        data = [row for row in reader]
    return data


def combine_and_sort_data(imu_data, odometry_data):
    combined_data = []

    # Combine IMU data
    for row in imu_data[1:]:  # Skip header
        combined_data.append({"timestamp": float(row[0]), "type": "imu", "data": row})

    # Combine Odometry data
    for row in odometry_data[1:]:  # Skip header
        combined_data.append(
            {"timestamp": float(row[0]), "type": "odometry", "data": row}
        )

    # Sort by timestamp
    combined_data.sort(key=lambda x: x["timestamp"])
    return combined_data


class CSVToROS2(Node):
    def __init__(self, data_dir):
        super().__init__("csv_to_ros2")

        # Publishers
        self.imu_pub = self.create_publisher(Imu, "/imu", 10)
        self.odometry_pub = self.create_publisher(Odometry, "/odometry", 10)
        self.camera_info_pub = self.create_publisher(CameraInfo, "/camera_info", 10)

        # Load CSV data
        imu_data = read_csv(f"{data_dir}/imu.csv")[1:]  # Skip header
        odometry_data = read_csv(f"{data_dir}/odometry.csv")[1:]  # Skip header
        self.camera_matrix = read_csv(f"{data_dir}/camera_matrix.csv")

        # Combine and sort data
        self.sorted_data = combine_and_sort_data(imu_data, odometry_data)
        self.current_index = 0

        # Set Camera Info
        self.camera_info_msg = CameraInfo()
        self.camera_info_msg.k = [float(x) for row in self.camera_matrix for x in row]

        # Publish camera info once at the start
        self.camera_info_pub.publish(self.camera_info_msg)

        # Start publishing sorted data
        self.start_time = time.time()
        self.initial_timestamp = self.sorted_data[0]["timestamp"]
        self.timer = self.create_timer(
            0.001, self.publish_data
        )  # High frequency, controlled inside

    def publish_data(self):
        if self.current_index >= len(self.sorted_data):
            self.get_logger().info("All data published.")
            self.destroy_timer(self.timer)
            return

        current_time = time.time()
        elapsed_time = current_time - self.start_time

        while (
            self.current_index < len(self.sorted_data)
            and (
                self.sorted_data[self.current_index]["timestamp"]
                - self.initial_timestamp
            )
            <= elapsed_time
        ):
            entry = self.sorted_data[self.current_index]
            timestamp = entry["timestamp"]

            if entry["type"] == "imu":
                imu_row = entry["data"]
                imu_msg = Imu()
                imu_msg.header.stamp.sec = int(timestamp)
                imu_msg.header.stamp.nanosec = int((timestamp - int(timestamp)) * 1e9)
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

            self.current_index += 1


def main(args=None):
    rclpy.init(args=args)

    # 명령줄 인자로 경로를 받음
    if len(sys.argv) < 2:
        print("Usage: python3 pub_imu_and_odometry.py <data_directory>")
        print(
            "Example: python3 pub_imu_and_odometry.py /ws/sample-data/8653a2142b/8653a2142b/"
        )
        rclpy.shutdown()
        return

    # 경로 설정
    data_dir = sys.argv[1]
    print(f"Using data directory: {data_dir}")

    # CSVToROS2 노드 생성 및 실행
    node = CSVToROS2(data_dir)
    rclpy.spin(node)

    # 종료 처리
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
