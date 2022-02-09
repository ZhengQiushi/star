#pragma once 

#include <vector>


namespace star {
    // template<typename key_type, typename value_type>
    struct myMove {
        std::vector<int32_t> keys;
        int32_t src_partition_id, dest_partition_id;
    };
}