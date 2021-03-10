//
// Created by pavlo on 3/4/21.
//

#ifndef ROSBAG2_TRANSPORT_MSG_FILTER_HH
#define ROSBAG2_TRANSPORT_MSG_FILTER_HH

#include <rosbag2_transport/bag_info.hh>
#include <memory>
#include <rclcpp/serialization.hpp>
#include "rosbag2_transport.hpp"

namespace rosbag2_transport {

    template<typename MsgType>
    struct MsgToEvent : public MsgToEventBase {
    private:
        typedef std::shared_ptr<MsgType> MsgPtr;
    public:
        void Set(const MsgPtr& msgPtr) {
            msgPtr_ = msgPtr;
        };
        void Set(const std::shared_ptr<rosbag2_storage::SerializedBagMessage>& serializedBagMessage) override {
            rclcpp::Serialization<MsgType> serialization;
            auto *msg = new rclcpp::SerializedMessage;
            msg->get_rcl_serialized_message() = *serializedBagMessage->serialized_data;
            msgPtr_ = std::make_shared<MsgType>();
            serialization.deserialize_message(msg, msgPtr_.get());
        }
    protected:
        MsgPtr msgPtr_;
    };

}

#endif //ROSBAG2_TRANSPORT_MSG_FILTER_HH
