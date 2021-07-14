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


class SomaticRosBag : rclcpp::Node {
public:
    SomaticRosBag() : Node("somatic_rosbag") {

    }

    void Record() {
        rosbag2_transport::StorageOptions storage_options{};
        rosbag2_transport::RecordOptions record_options{};

        // TODO: Get params

        storage_options.uri = get_parameter("uri").as_string();
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

        if (topics) {
            PyObject * topic_iterator = PyObject_GetIter(topics);
            if (topic_iterator != nullptr) {
                PyObject * topic;
                while ((topic = PyIter_Next(topic_iterator))) {
                    record_options.topics.emplace_back(PyUnicode_AsUTF8(topic));

                    Py_DECREF(topic);
                }
                Py_DECREF(topic_iterator);
            }
        }
        if (excludes) {
            PyObject * exclude_iterator = PyObject_GetIter(excludes);
            if (exclude_iterator != nullptr) {
                PyObject * exclude;
                while ((exclude = PyIter_Next(exclude_iterator))) {
                    record_options.excludes.emplace_back(PyUnicode_AsUTF8(exclude));
                    Py_DECREF(exclude);
                }
                Py_DECREF(exclude_iterator);
            }
        }
        record_options.rmw_serialization_format = std::string(serilization_format).empty() ?
                                                  rmw_get_serialization_format() :
                                                  serilization_format;

        // Specify defaults
        auto info = std::make_shared<rosbag2_cpp::Info>();
        auto reader = std::make_shared<rosbag2_cpp::Reader>(
                std::make_unique<rosbag2_cpp::readers::SequentialReader>());
        std::shared_ptr<rosbag2_cpp::Writer> writer;
        // Change writer based on recording options
        if (record_options.compression_format == "zstd") {
            writer = std::make_shared<rosbag2_cpp::Writer>(
                    std::make_unique<rosbag2_compression::SequentialCompressionWriter>(compression_options));
        } else {.
            writer = std::make_shared<rosbag2_cpp::Writer>(
                    std::make_unique<rosbag2_cpp::writers::SequentialWriter>());
        }

        rosbag2_transport::Rosbag2Transport transport(reader, writer, info);
        transport.init();
        transport.record(storage_options, record_options);
        transport.shutdown();
    }


private:
    char * uri = nullptr;
    char * storage_id = nullptr;
    char * serilization_format = nullptr;
    char * node_prefix = nullptr;
    char * compression_mode = nullptr;
    char * compression_format = nullptr;
    PyObject * qos_profile_overrides = nullptr;
    bool all = false;
    bool no_discovery = false;
    uint64_t polling_interval_ms = 100;
    unsigned long long max_bagfile_size = 0;  // NOLINT
    uint64_t max_cache_size = 0u;
    PyObject * topics = nullptr;
    PyObject * excludes = nullptr;
    bool include_hidden_topics = false;
};

int main (int argc, char **argv) {
    auto recorder = std::make_shared<SomaticRosBag>();
    rclcpp::spin(recorder);
}