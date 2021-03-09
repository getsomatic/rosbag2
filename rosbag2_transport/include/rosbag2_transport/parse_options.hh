//
// Created by pavlo on 3/4/21.
//

#ifndef ROSBAG2_TRANSPORT_PARSEOPTIONS_HH
#define ROSBAG2_TRANSPORT_PARSEOPTIONS_HH

#include <rosbag2_transport/msg_to_event_base.hh>

#include <map>

namespace rosbag2_transport {

    struct ParseOptions {
        std::map<std::string, std::shared_ptr<struct MsgToEventBase>> Convertors;

        [[nodiscard]] std::vector<BagInfo::Event> Convert(const std::string& typeName, const std::shared_ptr<rosbag2_storage::SerializedBagMessage>& sbm) const {
            auto conv = Convertors.find(typeName);
            conv->second->Set(sbm);
            return conv->second->Transform();
        }

        [[nodiscard]] bool ContainsType(const std::string& typeName) const {
            return Convertors.find(typeName) != Convertors.end();
        }
    };
}

#endif //ROSBAG2_TRANSPORT_PARSEOPTIONS_HH
