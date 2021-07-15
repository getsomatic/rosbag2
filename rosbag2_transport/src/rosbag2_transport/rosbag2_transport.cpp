// Copyright 2018, Bosch Software Innovations GmbH.
// Copyright 2020, TNG Technology Consulting GmbH.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <Python.h>
#include "rosbag2_transport/rosbag2_transport.hpp"

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "rclcpp/rclcpp.hpp"

#include <ctime>

#include "rosbag2_cpp/info.hpp"
#include "rosbag2_cpp/reader.hpp"
#include "rosbag2_cpp/readers/sequential_reader.hpp"
#include "rosbag2_cpp/typesupport_helpers.hpp"
#include "rosbag2_cpp/writer.hpp"
#include "rosbag2_cpp/writers/sequential_writer.hpp"

#include "rosbag2_transport/logging.hpp"

#include "formatter.hpp"
#include "player.hpp"
#include "recorder.hpp"
#include "rosbag2_node.hpp"

#include "rclcpp/serialization.hpp"

#include "bcr_core/tools/logging.hh"
#include <unistd.h>
namespace rosbag2_transport
{

Rosbag2Transport::Rosbag2Transport()
: reader_(std::make_shared<rosbag2_cpp::Reader>(
      std::make_unique<rosbag2_cpp::readers::SequentialReader>())),
  writer_(std::make_shared<rosbag2_cpp::Writer>(
      std::make_unique<rosbag2_cpp::writers::SequentialWriter>())),
  info_(std::make_shared<rosbag2_cpp::Info>())
{}

Rosbag2Transport::Rosbag2Transport(
  std::shared_ptr<rosbag2_cpp::Reader> reader,
  std::shared_ptr<rosbag2_cpp::Writer> writer,
  std::shared_ptr<rosbag2_cpp::Info> info)
: reader_(std::move(reader)), writer_(std::move(writer)), info_(std::move(info)) {}

void Rosbag2Transport::init()
{
//    std::cout << "\nAttach NOW!\n" << std::endl;
//    usleep(10000000);
//    std::cout << "\n\n\n INIT \n\n\n" << std::endl;
  //bcr::core::tools::logging::Logger("/opt/ros/foxy/bin/ros2").ExecutableLogLevel();
  if (!rclcpp::ok()) rclcpp::init(0, nullptr);
}

void Rosbag2Transport::shutdown()
{
  rclcpp::shutdown();
}

void Rosbag2Transport::record(
  const StorageOptions & storage_options, const RecordOptions & record_options)
{
  try {
    writer_->open(
      storage_options, {rmw_get_serialization_format(), record_options.rmw_serialization_format});

    auto transport_node = setup_node(record_options.node_prefix);

    Recorder recorder(writer_, transport_node);
    recorder.record(record_options);
  } catch (std::runtime_error & e) {
    ROSBAG2_TRANSPORT_LOG_ERROR("Error to record: %s", e.what());
  }
}

std::shared_ptr<Rosbag2Node> Rosbag2Transport::setup_node(
  std::string node_prefix,
  const std::vector<std::string> & topic_remapping_options)
{
  if (!transport_node_) {
    auto node_options = rclcpp::NodeOptions().arguments(topic_remapping_options);
    transport_node_ = std::make_shared<Rosbag2Node>(node_prefix + "_rosbag2", node_options);
  }
  return transport_node_;
}

void Rosbag2Transport::play_async(const StorageOptions &storage_options, const PlayOptions &play_options, double time) {
    ROSBAG2_TRANSPORT_LOG_INFO("rosbag_transport play_async");
    if (player_) {
        ROSBAG2_TRANSPORT_LOG_ERROR("previous player is still working");
    }
    try {
        ROSBAG2_TRANSPORT_LOG_INFO("openning reader");
        reader_->open(storage_options, {"", rmw_get_serialization_format()});
        ROSBAG2_TRANSPORT_LOG_INFO("setting up node");
        auto transport_node = setup_node(play_options.node_prefix);
        player_ = std::make_shared<rosbag2_transport::Player>(reader_, transport_node);
        ROSBAG2_TRANSPORT_LOG_INFO("player->play");
        player_->play(play_options, time);
    } catch (std::runtime_error &e) {
        ROSBAG2_TRANSPORT_LOG_ERROR("Error to play: %s", e.what());
    }
}

void Rosbag2Transport::play_async(const std::vector<StorageOptions> &storage_options, const PlayOptions &play_options, double time) {
    ROSBAG2_TRANSPORT_LOG_INFO("openning reader");
    reader_->open(storage_options[0], {"", rmw_get_serialization_format()});
    ROSBAG2_TRANSPORT_LOG_INFO("setting up node");
    auto transport_node = setup_node(play_options.node_prefix);
    player_ = std::make_shared<rosbag2_transport::Player>(reader_, transport_node);
    ROSBAG2_TRANSPORT_LOG_INFO("player->play");
    player_->play(storage_options, play_options, time);
}

void Rosbag2Transport::play(const StorageOptions &storage_options, const PlayOptions &play_options) {
    try {
        reader_->open(storage_options, {"", rmw_get_serialization_format()});

        auto transport_node = setup_node(play_options.node_prefix);

        Player player(reader_, transport_node);
        player.play(play_options);
        player.wait();
    } catch (std::runtime_error &e) {
        ROSBAG2_TRANSPORT_LOG_ERROR("Error to play: %s", e.what());
    }
}

void Rosbag2Transport::print_bag_info(const std::string & uri, const std::string & storage_id)
{
  rosbag2_storage::BagMetadata metadata;
  try {
    metadata = info_->read_metadata(uri, storage_id);
  } catch (std::runtime_error & e) {
    (void) e;
    ROSBAG2_TRANSPORT_LOG_ERROR_STREAM(
      "Could not read metadata for " << uri << ". Please specify "
        "the path to the folder containing an existing 'metadata.yaml' file or provide correct "
        "storage id if metadata file doesn't exist (see help).");
    return;
  }

  Formatter::format_bag_meta_data(metadata);
}


void Rosbag2Transport::pause(bool pause) {
    if (player_)
        player_->pause(pause);
}

void Rosbag2Transport::stop() {
    player_ = nullptr;
    ROSBAG2_TRANSPORT_LOG_INFO("player stopped");
}

double Rosbag2Transport::time() const {
    if (player_)
        return player_->time();
    return 0;
}

double Rosbag2Transport::speed() const {
    if (player_)
        return player_->speed();
    return 0;
}

void Rosbag2Transport::speed(double s) {
    if (player_)
        player_->speed(s);
}

void Rosbag2Transport::seek_forward(double time) {
    if (player_)
        player_->seek_forward(time);
}

BagInfo Rosbag2Transport::parse_info(const StorageOptions& option, const ParseOptions& parseOptions) {
    ROSBAG2_TRANSPORT_LOG_INFO("rosbag_transport parse_info");
    try {
        PlayOptions play_options;
        ROSBAG2_TRANSPORT_LOG_INFO("openning reader");
        ROSBAG2_TRANSPORT_LOG_INFO("setting up node");
        auto transport_node = setup_node(play_options.node_prefix);
        player_ = std::make_shared<rosbag2_transport::Player>(reader_, transport_node);
        ROSBAG2_TRANSPORT_LOG_INFO("player->parse");

        return player_->parse_info(option, parseOptions);
    } catch (std::runtime_error &e) {
        ROSBAG2_TRANSPORT_LOG_ERROR("Error to parse: %s", e.what());
        return BagInfo();
    }
}


}  // namespace rosbag2_transport
