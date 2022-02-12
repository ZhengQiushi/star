#pragma once 

#include <vector>


namespace star {
    template<typename key_type, typename value_type>
    struct MoveRecord {
        key_type key;
        int32_t key_size;
        value_type value;
        int32_t field_size;
        // int32_t commit_tid; 可以不用?
        // may come from different partition but to the same dest
        int32_t src_partition_id;
    };

    template<typename key_type, typename value_type>
    struct myMove {
        std::vector<MoveRecord<key_type, value_type> > records;
        // may come from different partition but to the same dest
        int32_t dest_partition_id;
    };
}