//
// Created by pavlo on 3/4/21.
//

#ifndef ROSBAG2_TRANSPORT_BCR_MSGS_TO_EVENTS_HH
#define ROSBAG2_TRANSPORT_BCR_MSGS_TO_EVENTS_HH

#include <rosbag2_transport/msg_to_event.hh>
#include <bcr_msgs/msg/robot_info.hpp>
#include <bcr_msgs/msg/status_list.hpp>

namespace rosbag2_transport {

    class RobotInfoToEvents : public MsgToEvent<bcr_msgs::msg::RobotInfo> {
    public:
        [[nodiscard]] std::vector<BagInfo::Event> Transform() const override;
    };

    class LidarProcessorResultEvents : public MsgToEvent<bcr_msgs::msg::StatusList> {
    public:
        [[nodiscard]] std::vector<BagInfo::Event> Transform() const override;
    };
}

#endif //ROSBAG2_TRANSPORT_BCR_MSGS_TO_EVENTS_HH