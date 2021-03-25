//
// Created by pavlo on 3/4/21.
//

#include "rosbag2_transport/bcr_msgs_to_events.hh"
#include <bcr_core/tools/status/entry.hh>

namespace rosbag2_transport {
    std::vector<BagInfo::Event> RobotInfoToEvents::Transform() const {
        BagInfo::Event event{};
        double nano = 1e-9;
        event.Start = msgPtr_->started * nano;
        event.End = msgPtr_->finished * nano;

        event.Type = static_cast<enum BagInfo::Event::Type>(msgPtr_->status_code);
        event.Message = msgPtr_->plan_name + "\n" + '(' +  std::to_string(msgPtr_->task_number) +
                        ')' + msgPtr_->task_name;
        event.Priority = 1;
        return {event};
    }

    std::vector<BagInfo::Event> LidarProcessorResultEvents::Transform() const {
        std::vector<BagInfo::Event> events;
        for(const auto& status : msgPtr_->statuses) {
            BagInfo::Event event{};
            event.Message = status.name + '\n' + status.value;
            event.Start = status.time;
            event.End = status.time + 0.01;
            auto statusCode = (bcr::core::tools::status::Status)status.status;
            switch (statusCode) {
                case bcr::core::tools::status::Status::OK:
                    continue;
                case bcr::core::tools::status::Status::WARN:
                    event.Priority = 3;
                    event.Type = BagInfo::Event::Warn;
                    break;
                case bcr::core::tools::status::Status::PAUSE:
                    event.Priority = 2;
                    event.Type = BagInfo::Event::Paused;
                    break;
                case bcr::core::tools::status::Status::STOP:
                    event.Priority = 2;
                    event.Type = BagInfo::Event::Fatal;
                    break;
            }
            events.push_back(event);
        }
        return events;
    }
}