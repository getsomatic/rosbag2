//
// Created by alex on 13.07.21.
//
#include <Python.h>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rclcpp/qos.hpp"

#include "rosbag2_compression/compression_options.hpp"
#include "rosbag2_compression/sequential_compression_reader.hpp"
#include "rosbag2_compression/sequential_compression_writer.hpp"
#include "rosbag2_cpp/info.hpp"
#include "rosbag2_cpp/reader.hpp"
#include "rosbag2_cpp/readers/sequential_reader.hpp"
#include "rosbag2_cpp/writer.hpp"
#include "rosbag2_cpp/writers/sequential_writer.hpp"
#include "rosbag2_storage/metadata_io.hpp"
#include "rosbag2_transport/rosbag2_transport.hpp"
#include "rosbag2_transport/record_options.hpp"
#include "rosbag2_transport/storage_options.hpp"
#include "rmw/rmw.h"
#include <boost/filesystem.hpp>

class SomaticRosBag : rclcpp::Node {
public:
    SomaticRosBag() : Node("somatic_rosbag") {
        declare_parameter("uri", "");
        declare_parameter("storage_id", "sqlite3");
        declare_parameter("max_bagfile_size", 100000000);
        declare_parameter("max_cache_size", 0);
        declare_parameter("all", true);
        declare_parameter("no_discovery", false);
        declare_parameter("polling_interval_ms", 100);
        declare_parameter("node_prefix", "_");
        declare_parameter("compression_mode", "none");
        declare_parameter("compression_format", "");
        declare_parameter("include_hidden_topics", false);
        declare_parameter("topics", {});
        declare_parameter("excludes", std::vector<std::string>({
                "/camera/aligned_depth_to_color/camera_info",
                "/camera/aligned_depth_to_color/image_raw",
                "/camera/color/camera_info",
                "/camera/color/image_raw",
                "/camera/color/image_raw/compressed",
                "/camera/color/image_raw/compressedDepth",
                "/camera/color/image_raw/theora",
                "/camera/depth/camera_info",
                "/camera/depth/image_rect_raw",
                "/camera/depth/image_rect_raw/compressed",
                "/camera/depth/image_rect_raw/compressedDepth",
                "/camera/depth/image_rect_raw/theora",
                "/camera/infra1/camera_info",
                "/camera/infra1/image_rect_raw",
                "/camera/infra1/image_rect_raw/compressed",
                "/camera/infra1/image_rect_raw/compressedDepth",
                "/camera/infra1/image_rect_raw/theora",
                "/camera/infra2/camera_info",
                "/camera/infra2/image_rect_raw",
                "/camera/infra2/image_rect_raw/compressed",
                "/camera/infra2/image_rect_raw/compressedDepth",
                "/camera/infra2/image_rect_raw/theora",
                "/camera/pointcloud",
                "/somatic/bcr/camera_point_cloud",
                "/somatic/bcr/projected_camera_points"
        }));
        declare_parameter("serialization_format", "");
    }

    void Record() {
        rosbag2_transport::StorageOptions storage_options{};
        rosbag2_transport::RecordOptions record_options{};

        auto uri = get_parameter("uri").as_string();
        if (uri.empty()) {
            auto home = std::getenv("SOMATIC_BAG_DIR");
            if (home) {
                uri = home;
            } else {
                RCLCPP_FATAL(get_logger(), "Could not get output dir, exiting");
                exit(1);
            }
        }


        time_t rawtime;
        struct tm * timeinfo;
        char buffer[80];

        time (&rawtime);
        timeinfo = localtime(&rawtime);

        strftime(buffer,sizeof(buffer),"bags/%Y/%m/%d/%H:%M:%S",timeinfo);
        std::string str(buffer);
        uri+='/'+ std::string(buffer);

        if (!boost::filesystem::create_directories(uri)) {
            RCLCPP_FATAL(get_logger(), "Could not create output dir [%s], exiting", uri.c_str());
            exit(1);
        }

        RCLCPP_INFO(get_logger(), "Output folder = [%s]", uri.c_str());
        storage_options.uri = uri;

        storage_options.storage_id = get_parameter("storage_id").as_string();
        storage_options.max_bagfile_size = get_parameter("max_bagfile_size").as_int();
        storage_options.max_cache_size =  get_parameter("max_cache_size").as_int();
        record_options.all = get_parameter("all").as_bool();
        record_options.is_discovery_disabled =  get_parameter("no_discovery").as_bool();
        record_options.topic_polling_interval = std::chrono::milliseconds(get_parameter("polling_interval_ms").as_int());
        record_options.node_prefix = get_parameter("node_prefix").as_string();
        record_options.compression_mode = get_parameter("compression_mode").as_string();
        record_options.compression_format = get_parameter("compression_format").as_string();
        record_options.include_hidden_topics = get_parameter("include_hidden_topics").as_bool();

        rosbag2_compression::CompressionOptions compression_options{
                record_options.compression_format,
                rosbag2_compression::compression_mode_from_string(record_options.compression_mode)
        };

        if (!record_options.all) {
            auto topics = get_parameter("topics").as_string_array();
            record_options.topics.insert(record_options.topics.begin(), topics.begin(), topics.end());
        }

        auto excludes = get_parameter("excludes").as_string_array();
        record_options.excludes.insert(record_options.excludes.begin(), excludes.begin(), excludes.end());

        auto sf = get_parameter("serialization_format").as_string();
        record_options.rmw_serialization_format = sf.empty() ? rmw_get_serialization_format() : sf;

        // Specify defaults
        auto info = std::make_shared<rosbag2_cpp::Info>();
        auto reader = std::make_shared<rosbag2_cpp::Reader>(
                std::make_unique<rosbag2_cpp::readers::SequentialReader>());
        std::shared_ptr<rosbag2_cpp::Writer> writer;
        // Change writer based on recording options
        if (record_options.compression_format == "zstd") {
            writer = std::make_shared<rosbag2_cpp::Writer>(
                    std::make_unique<rosbag2_compression::SequentialCompressionWriter>(compression_options));
        } else {
            writer = std::make_shared<rosbag2_cpp::Writer>(
                    std::make_unique<rosbag2_cpp::writers::SequentialWriter>());
        }

        rosbag2_transport::Rosbag2Transport transport(reader, writer, info);
        transport.init();
        transport.record(storage_options, record_options);
        transport.shutdown();
    }


};

int main (int argc, char **argv) {
    rclcpp::init(argc, argv);
    auto logger = rclcpp::get_logger("somatic_rosbag");
    auto recorder = std::make_shared<SomaticRosBag>();
    recorder->Record();
    return 0;
}