//
// Created by pavlo on 3/4/21.
//

#ifndef ROSBAG2_TRANSPORT_MSG_TO_EVENT_BASE_HH
#define ROSBAG2_TRANSPORT_MSG_TO_EVENT_BASE_HH

namespace rosbag2_transport {

    struct MsgToEventBase {
        [[nodiscard]] virtual std::vector<BagInfo::Event> Transform() const = 0;

        virtual void Set(const std::shared_ptr <rosbag2_storage::SerializedBagMessage> &serializedBagMessage) = 0;
    };
}

#endif //ROSBAG2_TRANSPORT_MSG_TO_EVENT_BASE_HH
