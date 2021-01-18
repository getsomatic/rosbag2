// Copyright 2018, Bosch Software Innovations GmbH.
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

#include "player.hpp"

#include <chrono>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rcl/graph.h"

#include "rclcpp/rclcpp.hpp"

#include "rcutils/time.h"

#include "rosbag2_cpp/reader.hpp"
#include "rosbag2_cpp/typesupport_helpers.hpp"

#include "rosbag2_storage/storage_filter.hpp"

#include "rosbag2_transport/logging.hpp"

#include "qos.hpp"
#include "rosbag2_node.hpp"
#include "replayable_message.hpp"

namespace
{
/**
 * Determine which QoS to offer for a topic.
 * The priority of the profile selected is:
 *   1. The override specified in play_options (if one exists for the topic).
 *   2. A profile automatically adapted to the recorded QoS profiles of publishers on the topic.
 *
 * \param topic_name The full name of the topic, with namespace (ex. /arm/joint_status).
 * \param topic_qos_profile_overrides A map of topic to QoS profile overrides.
 * @return The QoS profile to be used for subscribing.
 */
rclcpp::QoS publisher_qos_for_topic(
  const rosbag2_storage::TopicMetadata & topic,
  const std::unordered_map<std::string, rclcpp::QoS> & topic_qos_profile_overrides)
{
  using rosbag2_transport::Rosbag2QoS;
  auto qos_it = topic_qos_profile_overrides.find(topic.name);
  if (qos_it != topic_qos_profile_overrides.end()) {
    ROSBAG2_TRANSPORT_LOG_INFO_STREAM("Overriding QoS profile for topic " << topic.name);
    return Rosbag2QoS{qos_it->second};
  } else if (topic.offered_qos_profiles.empty()) {
    return Rosbag2QoS{};
  }

  const auto profiles_yaml = YAML::Load(topic.offered_qos_profiles);
  const auto offered_qos_profiles = profiles_yaml.as<std::vector<Rosbag2QoS>>();
  return Rosbag2QoS::adapt_offer_to_recorded_offers(topic.name, offered_qos_profiles);
}
}  // namespace

namespace rosbag2_transport
{

const std::chrono::milliseconds
Player::queue_read_wait_period_ = std::chrono::milliseconds(100);

Player::Player(
  std::shared_ptr<rosbag2_cpp::Reader> reader, std::shared_ptr<Rosbag2Node> rosbag2_transport)
: reader_(std::move(reader)), rosbag2_transport_(rosbag2_transport)
{}

bool Player::is_storage_completely_loaded() const
{
  if (storage_loading_future_.valid() &&
    storage_loading_future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
  {
    storage_loading_future_.get();
  }
  return !storage_loading_future_.valid();
}

void Player::play(const PlayOptions & options)
{
  topic_qos_profile_overrides_ = options.topic_qos_profile_overrides;
  prepare_publishers(options);

  storage_loading_future_ = std::async(
    std::launch::async,
    [this, options]() {load_storage_content(options);});

  wait_for_filled_queue(options);

  play_messages_from_queue(options);
    const std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
}

void Player::wait_for_filled_queue(const PlayOptions & options) const
{
  while (
    message_queue_.size_approx() < options.read_ahead_queue_size &&
    !is_storage_completely_loaded() && rclcpp::ok())
  {
    std::this_thread::sleep_for(queue_read_wait_period_);
  }
}

void Player::load_storage_content(const PlayOptions & options)
{
  TimePoint time_first_message;

  ReplayableMessage message;
  if (reader_->has_next()) {
    message.message = reader_->read_next();
    message.time_since_start = std::chrono::nanoseconds(0);
    time_first_message = TimePoint(std::chrono::nanoseconds(message.message->time_stamp));
    message_queue_.enqueue(message);
  }

  auto queue_lower_boundary =
    static_cast<size_t>(options.read_ahead_queue_size * read_ahead_lower_bound_percentage_);
  auto queue_upper_boundary = options.read_ahead_queue_size;

    while (reader_->has_next() && rclcpp::ok()) {
        if (message_queue_.size_approx() < queue_lower_boundary) {
            enqueue_up_to_boundary(time_first_message, queue_upper_boundary);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        mutex_.lock();
        bool exit = exit_;
        mutex_.unlock();

        if (exit)
            break;
    }
}

void Player::enqueue_up_to_boundary(const TimePoint & time_first_message, uint64_t boundary)
{
  ReplayableMessage message;
  for (size_t i = message_queue_.size_approx(); i < boundary; i++) {
    if (!reader_->has_next()) {
      break;
    }
    message.message = reader_->read_next();
    message.time_since_start =
      TimePoint(std::chrono::nanoseconds(message.message->time_stamp)) - time_first_message;

    message_queue_.enqueue(message);
  }
}

void Player::play_messages_from_queue(const PlayOptions & options)
{
    ROSBAG2_TRANSPORT_LOG_INFO_STREAM("topics to filter size: " << options.topics_to_filter.size());
    bool rdy = false;
    ROSBAG2_TRANSPORT_LOG_INFO("waiting player is ready");
    while (!rdy){ //todo: events
        mutex_.lock();
        rdy |= ready_;
        mutex_.unlock();
        std::this_thread::sleep_for (std::chrono::milliseconds (10));
    }
    ROSBAG2_TRANSPORT_LOG_INFO("player is ready");

    time_ = 0;
    bool pause;
    bool exit;
    do {
        double stt = 0;
        mutex_.lock();
        pause = pause_;
        exit = exit_;
        stt = skip_till_time_;
        mutex_.unlock();
        if (exit || !rclcpp::ok())
            break;
        if (!pause){
            int ms = 10;
            double sec = 0.001 * ms;
            if (stt > 0)
                time_ = stt;
            else
                time_ += sec * speed_;
            ReplayableMessage message;
            std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            auto timeDur = std::chrono::duration<double>(time_);
            if (upcommingMsg_ && upcommingMsg_->time_since_start <= timeDur && rclcpp::ok()){
                publishers_[upcommingMsg_->message->topic_name]->publish(upcommingMsg_->message->serialized_data);
                upcommingMsg_ = nullptr;
            }


            while (!upcommingMsg_ && message_queue_.try_dequeue(message)){
                if (message.time_since_start <= timeDur){
                    if (stt == 0)
                        publishers_[message.message->topic_name]->publish(message.message->serialized_data);
                }
                else {
                    upcommingMsg_ = std::make_shared<ReplayableMessage>(message);
                }
            }

            if (is_storage_completely_loaded() && !upcommingMsg_){
                ROSBAG2_TRANSPORT_LOG_WARN_STREAM("playing is finished");
                break;
            }

            if (!upcommingMsg_ && rclcpp::ok()) {
                ROSBAG2_TRANSPORT_LOG_WARN("Message queue starved. Messages will be delayed. Consider "
                                           "increasing the --read-ahead-queue-size option.");
            }
            if (stt > 0){
                mutex_.lock();
                skip_till_time_ = 0;
                mutex_.unlock();
            }
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            //ROSBAG2_TRANSPORT_LOG_WARN("paused");
        }


    } while (true);
    mutex_.lock();
    finished_ = true;
    mutex_.unlock();
}


void Player::prepare_publishers(const PlayOptions & options)
{
  rosbag2_storage::StorageFilter storage_filter;
  storage_filter.topics = options.topics_to_filter;
  reader_->set_filter(storage_filter);

  auto topics = reader_->get_all_topics_and_types();
  for (const auto & topic : topics) {
    auto topic_qos = publisher_qos_for_topic(topic, topic_qos_profile_overrides_);
    publishers_.insert(
      std::make_pair(
        topic.name, rosbag2_transport_->create_generic_publisher(
          topic.name, topic.type, topic_qos)));
  }
}


Player::~Player() {
    ROSBAG2_TRANSPORT_LOG_INFO("Player destructor called");
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        exit_ = true;
    }
    ROSBAG2_TRANSPORT_LOG_INFO("waiting for queue thread");
    queueThread_.join();
    ROSBAG2_TRANSPORT_LOG_INFO("waiting for storage loading");
    if (storage_loading_future_.valid())
        storage_loading_future_.wait();
    ROSBAG2_TRANSPORT_LOG_INFO("destructor is finished");
}

void Player::pause(bool pause) {
    const std::lock_guard<std::mutex> lock(mutex_);
    pause_ = pause;
}

bool Player::finished() {
    const std::lock_guard<std::mutex> lock(mutex_);
    return finished_;
}

void Player::wait() {
    queueThread_.join();
}

double Player::time() const {
    return time_;
}

void Player::seek_forward(double time) {
    if (time > time_)
        skip_till_time_ = time;
}

void Player::speed(double s) {
    speed_ = s;
}

double Player::speed() const {
    return speed_;
}

}  // namespace rosbag2_transport
