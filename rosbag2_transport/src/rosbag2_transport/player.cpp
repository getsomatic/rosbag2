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
#include <rosbag2_transport/bag_info.hh>

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
#include <bcr_msgs/msg/robot_info.hpp>
#include <rosbag2_transport/parse_options.hh>

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

void Player::finished_callback(const std::function<void()> & finishedCallback)
{
  finishedCallback_ = finishedCallback;
}

void Player::play(const PlayOptions & options, double time)
{
  topic_qos_profile_overrides_ = options.topic_qos_profile_overrides;
  prepare_publishers(options);
  time_ = 0;

  storage_loading_future_ = std::async(
    std::launch::async,
    [this, options]() {load_storage_content(options);});

  wait_for_filled_queue(options);

  seek_forward(time);
  queueThread_ = std::make_shared<std::thread>([this] { play_messages_from_queue_no_param(); });
  const std::lock_guard<std::mutex> lock(mutex_);
  ready_ = true;
  finished_ = false;
}

void Player::play(const std::vector<StorageOptions> &storage_options, const PlayOptions &options, double time) {
    topic_qos_profile_overrides_ = options.topic_qos_profile_overrides;
    prepare_publishers(options);
    time_ = 0;

    storage_loading_future_ = std::async(
            std::launch::async,
            [this, options, storage_options]() {
                loading_next_bag_ = true;
                for(size_t i = 1; i < storage_options.size(); i++) {
                    ROSBAG2_TRANSPORT_LOG_INFO("opening reader");
                    load_storage_content(options);
                    reader_->open(storage_options[i], {"", rmw_get_serialization_format()});
                    prepare_publishers(options);
                }
                load_storage_content(options);
                loading_next_bag_ = false;
            });

    wait_for_filled_queue(options);

    seek_forward(time);
    queueThread_ = std::make_shared<std::thread>([this] { play_messages_from_queue_no_param(); });
    const std::lock_guard<std::mutex> lock(mutex_);
    ready_ = true;
    finished_ = false;
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
  ReplayableMessage last_msg;
  while (reader_->has_next() && rclcpp::ok()) {
    if (message_queue_.size_approx() < queue_lower_boundary) {
      last_msg = enqueue_up_to_boundary(time_first_message, queue_upper_boundary);
      last_msgs_time_ = last_msg.time_since_start;
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

ReplayableMessage Player::enqueue_up_to_boundary(const TimePoint & time_first_message, uint64_t boundary)
{
  ReplayableMessage message;
  for (size_t i = message_queue_.size_approx(); i < boundary; i++) {
    if (!reader_->has_next()) {
      break;
    }
    message.message = reader_->read_next();
    message.time_since_start =
      TimePoint(std::chrono::nanoseconds(message.message->time_stamp)) - time_first_message + last_msgs_time_;

    message_queue_.enqueue(message);
  }
    return message;
}

void Player::play_messages_from_queue_no_param() {
    PlayOptions options;
    play_messages_from_queue(options);
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
        if(exit_) {
            mutex_.lock();
            finished_ = true;
            finishedCallback_();
            mutex_.unlock();
            return;
        }
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
            if (upcomming_msg_ && upcomming_msg_->time_since_start <= timeDur && rclcpp::ok()){
                publishers_[upcomming_msg_->message->topic_name]->publish(upcomming_msg_->message->serialized_data);
                upcomming_msg_ = nullptr;
            }


            while (!upcomming_msg_ && message_queue_.try_dequeue(message)){
                if (message.time_since_start <= timeDur){
                    if (stt == 0)
                        publishers_[message.message->topic_name]->publish(message.message->serialized_data);
                }
                else {
                    upcomming_msg_ = std::make_shared<ReplayableMessage>(message);
                }
            }

            if (is_storage_completely_loaded() && !upcomming_msg_ && !loading_next_bag_){
                ROSBAG2_TRANSPORT_LOG_WARN_STREAM("playing is finished");
                break;
            }

            if (!upcomming_msg_ && rclcpp::ok()) {
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
    finishedCallback_();
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
    if(queueThread_ && queueThread_->joinable())
        queueThread_->join();
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
    if(queueThread_ && queueThread_->joinable())
        queueThread_->join();
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

BagInfo Player::parse_info(const StorageOptions& storageOptions, const ParseOptions& parseOptions) {
    BagInfo info;

    const auto sqlite_storage = std::make_unique<rosbag2_storage_plugins::SqliteStorage>();
    reader_->open(storageOptions, {"", rmw_get_serialization_format()});

    double nano = 1e-9;
    info.Start = reader_->get_metadata().starting_time.time_since_epoch().count() * nano;

    info.End = info.Start + reader_->get_metadata().duration.count() * nano;

    rosbag2_storage::StorageFilter filter;
    std::map<std::string, std::string> topicToType;
    for(const auto& topicAndType : reader_->get_all_topics_and_types())
        if(parseOptions.ContainsType(topicAndType.type)) {
            filter.topics.push_back(topicAndType.name);
            topicToType.insert({topicAndType.name, topicAndType.type});
        }
    reader_->set_filter(filter);
    if(topicToType.empty())
        return info;
//    BagInfo::Event *unknownEvent = nullptr;
    while(reader_->has_next()) {
        try {
            auto next = reader_->read_next();

            auto events = parseOptions.Convert(topicToType[next->topic_name], next);
            info.Events.insert(events.begin(), events.end());

//            if(unknownEvent) {
//                unknownEvent->End = events.Start;
//                //info.Events.insert(*unknownEvent);
//                delete unknownEvent;
//                unknownEvent = nullptr;
//            }
        } catch(std::exception& e) {
//            if(!unknownEvent) {
//                unknownEvent = new BagInfo::Event;
//                if (!info.Events.empty())
//                    unknownEvent->Start = info.Events.end()->End;
//                else
//                    unknownEvent->Start = info.Start;
//                unknownEvent->Message = "Fog of cataclysm";
//                unknownEvent->Type = BagInfo::Event::Type::Unknown;
//            }
            auto msg = e.what();
            std::cout << msg << std::endl;
            ROSBAG2_TRANSPORT_LOG_WARN("Memory gap, filling with unknown event");
        }
    }
//    if(unknownEvent) {
//        unknownEvent->End = info.End;
//        //info.Events.insert(*unknownEvent);
//        delete unknownEvent;
//    }

    return info;
}

}  // namespace rosbag2_transport
