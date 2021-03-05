//
// Created by pavlo on 2/22/21.
//

#ifndef ROSBAG2_TRANSPORT_BAGINFO_HH
#define ROSBAG2_TRANSPORT_BAGINFO_HH

#include <set>

namespace rosbag2_transport {

    struct BagInfo {
        struct Event {
            enum Type : uint8_t {
                Ok,
                OkPlanStart,
                OkPlanFinished,
                Error,
                Paused,
                Stopped,
                Unknown,
                Warn,
                Fatal
            } Type;
            double Start;
            double End;
            std::string Message;

            bool operator<(const Event& e) const{
                return Start < e.Start;
            }
            uint8_t Priority = 0;
        };
        double Start;
        double End;
        std::set<Event> Events;

        bool operator<(const BagInfo& bi) const{
            return Start < bi.Start;
        }
    };

   // bool BagInfo::Event::operator<(const Event& e) const
}
#endif //ROSBAG2_TRANSPORT_BAGINFO_HH
