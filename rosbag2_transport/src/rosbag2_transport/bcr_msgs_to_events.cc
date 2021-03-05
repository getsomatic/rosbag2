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
        double nano = 1e-9;
        for(const auto& status : msgPtr_->statuses) {
            BagInfo::Event event{};
            event.Message = status.name + '\n' + status.value;
            event.Start = status.time * nano;
            event.End = status.time + 0.01;
            event.Priority = 2;
            auto statusCode = (bcr::core::tools::status::Status)status.status;
            switch (statusCode) {
                case bcr::core::tools::status::OK:
                    event.Type = BagInfo::Event::Ok;
                    break;
                case bcr::core::tools::status::WARN:
                    event.Type = BagInfo::Event::Warn;
                    break;
                case bcr::core::tools::status::ERROR:
                    event.Type = BagInfo::Event::Error;
                    break;
                case bcr::core::tools::status::FATAL:
                    event.Type = BagInfo::Event::Fatal;
                    break;
            }
            events.push_back(event);
        }
        return events;
    }
}