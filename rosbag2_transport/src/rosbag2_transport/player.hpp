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

#ifndef ROSBAG2_TRANSPORT__PLAYER_HPP_
#define ROSBAG2_TRANSPORT__PLAYER_HPP_

#include <chrono>
#include <future>
#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <rosbag2_transport/bag_info.hh>
#include <rosbag2_transport/storage_options.hpp>

#include <rosbag2_storage_default_plugins/sqlite/sqlite_storage.hpp>
#include <rosbag2_transport/parse_options.hh>

#include "moodycamel/readerwriterqueue.h"

#include "rclcpp/qos.hpp"

#include "rosbag2_transport/play_options.hpp"

#include "replayable_message.hpp"

using TimePoint = std::chrono::time_point<std::chrono::high_resolution_clock>;

namespace rosbag2_cpp {
    class Reader;
}  // namespace rosbag2_cpp

namespace rosbag2_transport {

    class GenericPublisher;

    class Rosbag2Node;

    class Player {
    public:
        explicit Player(
                std::shared_ptr<rosbag2_cpp::Reader> reader,
                std::shared_ptr<Rosbag2Node> rosbag2_transport);

        ~Player();

        void play(const PlayOptions &options, double time = 0.0);

        void play(const std::vector<StorageOptions> &storage_options, const PlayOptions &options, double time = 0.0);

        void finished_callback(const std::function<void()> & finishedCallback = [](){});

        void pause(bool pause);

        bool finished();

        void wait();

        double time() const;

        double speed() const;

        void speed(double s);

        void seek_forward(double time);

        BagInfo parse_info(const StorageOptions& storageOptions, const ParseOptions& parseOptions);
    private:
        void load_storage_content(const PlayOptions &options);

        bool is_storage_completely_loaded() const;

        ReplayableMessage enqueue_up_to_boundary(const TimePoint &time_first_message, uint64_t boundary);

        void wait_for_filled_queue(const PlayOptions &options) const;

        void play_messages_from_queue(const PlayOptions &options);

        void play_messages_from_queue_no_param();

        void prepare_publishers(const PlayOptions &options);

        static constexpr double read_ahead_lower_bound_percentage_ = 0.9;
        static const std::chrono::milliseconds queue_read_wait_period_;

        std::shared_ptr<rosbag2_cpp::Reader> reader_;
        moodycamel::ReaderWriterQueue<ReplayableMessage> message_queue_;
        std::chrono::time_point<std::chrono::system_clock> start_time_;
        mutable std::future<void> storage_loading_future_;
        std::shared_ptr<Rosbag2Node> rosbag2_transport_;
        std::unordered_map<std::string, std::shared_ptr<GenericPublisher>> publishers_;
        std::unordered_map<std::string, rclcpp::QoS> topic_qos_profile_overrides_;
        double time_ = 0;
        std::shared_ptr<ReplayableMessage> upcomming_msg_ = nullptr;
        double speed_ = 1.0;
        double skip_till_time_ = 0;

        std::chrono::nanoseconds last_msgs_time_ = std::chrono::nanoseconds(0);

        std::atomic_bool ready_ {false};
        std::atomic_bool pause_ {false};
        std::atomic_bool exit_ {false};
        std::atomic_bool finished_ {false};
        std::atomic_bool loading_next_bag_ {false};
        std::mutex mutex_;
        std::shared_ptr<std::thread> queueThread_;

        std::function<void()> finishedCallback_ = [](){};
    };

}  // namespace rosbag2_transport

#endif  // ROSBAG2_TRANSPORT__PLAYER_HPP_
