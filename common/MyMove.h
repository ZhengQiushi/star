#pragma once

#include "core/Context.h"
#include "benchmark/ycsb/Schema.h"
#include "core/Defs.h"

#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <map>

#include <glog/logging.h>

namespace star
{
    // template <typename key_type, typename value_type>
    // struct MoveRecord
    // {
    //     key_type key;
    //     int32_t key_size;
    //     value_type value;
    //     int32_t field_size;
    //     // int32_t commit_tid; 可以不用?
    //     // may come from different partition but to the same dest
    //     int32_t src_partition_id;

    //     void set_key(key_type k, int32_t src_p_id){
    //         key = k;
    //         src_partition_id = src_p_id;
    //     }
    // };
    template <class Workload>
    struct MoveRecord{
        using WorkloadType = Workload;

        int32_t table_id;
        int32_t key_size;
        int32_t field_size;
        int32_t src_partition_id;
        u_int64_t record_key_;

        union key_ {
            key_(){
                // can't be default
            }
            ycsb::ycsb::key ycsb_key;
            tpcc::warehouse::key w_key;
            tpcc::district::key d_key;
            tpcc::customer::key c_key;
            tpcc::stock::key s_key;
        } key;
        
        union val_ {
            val_(){
                ;
            }           
            star::ycsb::ycsb::value ycsb_val;
            tpcc::warehouse::value w_val;
            tpcc::district::value d_val;
            tpcc::customer::value c_val;
            tpcc::stock::value s_val;
        } value;

        MoveRecord(){
            table_id = 0;
            memset(&key, 0, sizeof(key));
            memset(&value, 0, sizeof(value));
        }
        ~MoveRecord(){
            table_id = 0;
            memset(&key, 0, sizeof(key));
            memset(&value, 0, sizeof(value));
        }
        void set_real_key(uint64_t record_key){
            /**
             * @brief Construct a new DCHECK object
             * 
             */
            record_key_ = record_key;

            this->key_size = sizeof(uint64_t);
            
            myTestSet hihi = WorkloadType::which_workload;

            if(WorkloadType::which_workload == myTestSet::YCSB){
                this->key.ycsb_key = record_key;
                this->field_size = ClassOf<ycsb::ycsb::value>::size();
            } else if(WorkloadType::which_workload == myTestSet::TPCC){
                int32_t table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);
                this->table_id = table_id;

                int32_t w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
                int32_t d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
                int32_t c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
                int32_t s_id = (record_key & RECORD_COUNT_OL_ID_VALID);
                switch (table_id)
                {
                case tpcc::warehouse::tableID:
                    this->key.w_key = tpcc::warehouse::key(w_id); // res = new tpcc::warehouse::key(content);
                    this->field_size = ClassOf<tpcc::warehouse::value>::size();
                    break;
                case tpcc::district::tableID:
                    this->key.d_key = tpcc::district::key(w_id, d_id);
                    this->field_size = ClassOf<tpcc::district::value>::size();
                    break;
                case tpcc::customer::tableID:
                    this->key.c_key = tpcc::customer::key(w_id, d_id, c_id);
                    this->field_size = ClassOf<tpcc::customer::value>::size();
                    break;
                case tpcc::stock::tableID:
                    this->key.s_key = tpcc::stock::key(w_id, s_id);
                    this->field_size = ClassOf<tpcc::stock::value>::size();
                    break;
                default:
                    DCHECK(false);
                    break;
                }
            }
            return;

        }

        void set_real_value(uint64_t record_key, const void* record_val){            
            if(WorkloadType::which_workload == myTestSet::YCSB){
                this->table_id = ycsb::ycsb::tableID;

                const auto &v = *static_cast<const ycsb::ycsb::value *>(record_val);  
                this->value.ycsb_val = v;
            } else if(WorkloadType::which_workload == myTestSet::TPCC){
                int32_t table_id = (record_key >> RECORD_COUNT_TABLE_ID_OFFSET);
                this->table_id = table_id;

                // int32_t w_id = (record_key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
                // int32_t d_id = (record_key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
                // int32_t c_id = (record_key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
                // int32_t s_id = (record_key & RECORD_COUNT_OL_ID_VALID);
                switch (table_id)
                {
                case tpcc::warehouse::tableID:{
                    const auto &v = *static_cast<const tpcc::warehouse::value *>(record_val);  
                    this->value.w_val = v;
                    // this->key.w_key = tpcc::warehouse::key(w_id); // res = new tpcc::warehouse::key(content);
                    // this->field_size = ClassOf<tpcc::warehouse::value>::size();
                    break;
                }
                case tpcc::district::tableID:{
                    const auto &v = *static_cast<const tpcc::district::value *>(record_val);  
                    this->value.d_val = v;
                    // this->key.d_key = tpcc::district::key(w_id, d_id);
                    // this->field_size = ClassOf<tpcc::district::value>::size();
                    break;
                }
                case tpcc::customer::tableID:{
                    const auto &v = *static_cast<const tpcc::customer::value *>(record_val);  
                    this->value.c_val = v;
                    // this->key.c_key = tpcc::customer::key(w_id, d_id, c_id);
                    // this->field_size = ClassOf<tpcc::customer::value>::size();
                    break;
                }
                case tpcc::stock::tableID:{
                    const auto &v = *static_cast<const tpcc::stock::value *>(record_val);  
                    this->value.s_val = v;
                    // this->key.s_key = tpcc::stock::key(w_id, s_id);
                    // this->field_size = ClassOf<tpcc::stock::value>::size();
                    break;
                }
                default:
                    DCHECK(false);
                    break;
                }
            }
            return;

        }
    
    };

    template <class Workload>
    struct myMove
    {   
    /**
     * @brief a bunch of record that needs to be transformed 
     * 
     */ 
        using WorkloadType = Workload;

        std::vector<MoveRecord<WorkloadType>> records;
        // std::vector<MoveRecord<tpcc::warehouse::key, tpcc::warehouse::value> > tpcc_warehouse_records;
        // std::vector<MoveRecord<tpcc::district::key, tpcc::district::value> > tpcc_district_records;
        // std::vector<MoveRecord<tpcc::customer::key, tpcc::customer::value> > tpcc_customer_records;
        // std::vector<MoveRecord<tpcc::stock::key, tpcc::stock::value> > tpcc_stack_records;

        // std::vector<MoveRecord<key_type, value_type> > records;
        // may come from different partition but to the same dest
        int32_t dest_partition_id;
        // size_t cur_move_record_num;

        myMove(){
            reset(); 
        }
        // void push_back(u_int64_t key, int32_t p_id){

            // int32_t table_id = (key & RECORD_COUNT_TABLE_ID_VALID) >> RECORD_COUNT_TABLE_ID_OFFSET;
            // int32_t w_id = (key & RECORD_COUNT_W_ID_VALID) >> RECORD_COUNT_W_ID_OFFSET;
            // int32_t d_id = (key & RECORD_COUNT_D_ID_VALID) >> RECORD_COUNT_D_ID_OFFSET;
            // int32_t c_id = (key & RECORD_COUNT_C_ID_VALID) >> RECORD_COUNT_C_ID_OFFSET;
            // int32_t s_id = (key & RECORD_COUNT_OL_ID_VALID);

            // switch(table_id){
            //     case 0:{ // ycsb
            //         // if(Workload)
            //         MoveRecord<ycsb::ycsb::key, ycsb::ycsb::value> rec;
            //         auto tmp_key = ycsb::ycsb::key(key);
            //         rec.set_key(tmp_key, p_id);
            //         ycsb_records.push_back(rec);

            //         // MoveRecord<tpcc::warehouse::key, tpcc::warehouse::value> rec;
            //         // auto tmp_key = tpcc::warehouse::key(w_id);
            //         // rec.set_key(tmp_key, p_id);
            //         // tpcc_warehouse_records.push_back(rec);
            //         break;
            //     }
            //     case tpcc::district::tableID:{
            //         MoveRecord<tpcc::district::key, tpcc::district::value> rec;
            //         auto tmp_key = tpcc::district::key(w_id, d_id);
            //         rec.set_key(tmp_key, p_id);
            //         tpcc_district_records.push_back(rec);
            //         break;
            //     }
            //     case tpcc::customer::tableID:{
            //         MoveRecord<tpcc::customer::key, tpcc::customer::value> rec;
            //         auto tmp_key = tpcc::customer::key(w_id, d_id, c_id);
            //         rec.set_key(tmp_key, p_id);
            //         tpcc_customer_records.push_back(rec);
            //         break;
            //     }
            //     case tpcc::stock::tableID:{
            //         MoveRecord<tpcc::stock::key, tpcc::stock::value> rec;
            //         auto tmp_key = tpcc::stock::key(w_id, s_id);
            //         rec.set_key(tmp_key, p_id);
            //         tpcc_stack_records.push_back(rec);
            //         break;
            //     }
            // }     
            // cur_move_record_num ++; 
        // }
        void reset()
        {
            records.clear();

            // tpcc_warehouse_records.clear();
            // tpcc_district_records.clear();
            // tpcc_customer_records.clear();
            // tpcc_stack_records.clear();
            dest_partition_id = -1;
            // cur_move_record_num = 0;
        }
    };

    static const int32_t cross_txn_weight = 50;
    static const int32_t find_clump_look_ahead = 5;

    struct Node
    {
        u_int64_t from, to;
        int32_t from_p_id, to_p_id;
        int degree;
        int on_same_coordi;
    };
    struct myTuple{
        u_int64_t key;
        int32_t p_id;
        int32_t degree;
        myTuple(u_int64_t key_, int32_t p_id_, int32_t degree_){
            key = key_;
            p_id = p_id_;
            degree = degree_;
        }
        bool operator< (const myTuple& n2) const {    
            return  n2.degree < this->degree ;  //"<"为从大到小排列，">"为从小到大排列    
        }  

        bool operator==(const myTuple& n2) const {
            return this->key == n2.key;
        }
    };

    bool nodecompare(const Node &lhs, const Node &rhs)
    {
        // if(lhs.on_same_coordi < rhs.on_same_coordi){
        // 优先是跨分区, 关联度高在前面
        if (lhs.on_same_coordi == rhs.on_same_coordi)
        {
            //
            return lhs.degree > rhs.degree;
        }
        return lhs.on_same_coordi < rhs.on_same_coordi;
    }

    struct NodeCompare
    {
        bool operator()(const Node &lhs, const Node &rhs)
        {
            return nodecompare(lhs, rhs);
        }
    };

    // template<typename T,
    //          typename Sequence = std::vector<T>,
    //          typename Compare = std::less<typename Sequence::value_type> > priority_queue<Node, std::vector<Node>, NodeCompare>
    class fixed_priority_queue : public std::vector<myTuple> // <T,Sequence,Compare>
    {
        /* 固定大小的大顶堆 */
    public:
        fixed_priority_queue(unsigned int size = 50000) : fixed_size(size) {}
        void push_back(const myTuple &x)
        {   
            // 
            // If we've reached capacity, find the FIRST smallest object and replace
            // it if 'x' is larger
            size_t num = this->size();
            auto beg = this->begin();
            // auto end = this->end();

            size_t i;
            for ( i = 0; i < num; i++){
                myTuple& cur_tuple = *(beg + i);
                if (cur_tuple == x){
                    // 找到了
                    cur_tuple = x;
                    break;
                }
            }

            if (i == num){
                if(this->size() < fixed_size){
                    std::vector<myTuple>::push_back(x);
                } else {
                    myTuple& min = *(beg + num - 1);
                    if (min < x){
                        min = x;
                    }
                }
            }

            std::sort(this->begin(), this->end());
        }

    private:
        // fixed_priority_queue() {} // Construct with size only.
        const unsigned int fixed_size;
        // Prevent heap allocation
        void *operator new(size_t);
        void *operator new[](size_t);
        void operator delete(void *);
        void operator delete[](void *);
    };

    template <class Workload>
    class Clay
    {   
        using WorkloadType = Workload;
        using myKeyType = u_int64_t;
    public: // unordered_  unordered_
        Clay(std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node> >& record_d):
        record_degree(record_d){
        }

        void find_clump(std::vector<myMove<WorkloadType>> &moves)
        {
            /***
             * @brief find all the records to move
             *        each moves is the set of records which may from different 
             *        partition but will be transformed to the same destination
            */
            average_load = 0;
            int32_t overloaded_partition_id = find_overloaded_partition(average_load);
            if(overloaded_partition_id == -1){
                return;
            }
            // check if there is the hottest tuple on this partition
            if (big_node_heap.find(overloaded_partition_id) == big_node_heap.end()){
                return;
            }
            cur_load = load_partition[overloaded_partition_id];

            //
            myMove<WorkloadType> C_move, tmp_move;
            auto cur_node_ptr = big_node_heap[overloaded_partition_id].begin();
            int32_t look_ahead = find_clump_look_ahead;
            // the cur_load will change as the clump keeps expand
            while (cur_load + cost_delta_for_sender(C_move, overloaded_partition_id) > average_load){
                if (tmp_move.records.empty()){
                    // hottest tuple
                    if (cur_node_ptr == big_node_heap[overloaded_partition_id].end()){ 
                        // find next hottest edge!
                        break;
                    }

                    myTuple &cur_node = *cur_node_ptr;
                    do {
                        cur_node = *cur_node_ptr;
                        cur_node_ptr++;
                        if(move_tuple_id.find(cur_node.key) == move_tuple_id.end()){
                            // have not moved yet
                            break;
                        }
                    } while(cur_node_ptr != big_node_heap[overloaded_partition_id].end());

                    if(move_tuple_id.find(cur_node.key) != move_tuple_id.end()){
                        // all heap has been used 
                        if(moves.size() == 0){
                            LOG(INFO) << "why none";
                        }
                        break;
                    }

                    MoveRecord<WorkloadType> rec;
                    if (cur_node.p_id == overloaded_partition_id){
                        rec.set_real_key(cur_node.key);
                        rec.src_partition_id = cur_node.p_id;
                    }
                    tmp_move.records.push_back(rec);
                    tmp_move.dest_partition_id = overloaded_partition_id;

                    // dest-partition
                    tmp_move.dest_partition_id = initial_dest_partition(tmp_move);// tmp_move.dest_partition_id = rec.src_partition_id == cur_node.from_p_id ? cur_node.to_p_id : cur_node.from_p_id;
                    DCHECK(tmp_move.dest_partition_id != -1);

                    // get its neighbor cached
                    get_neighbor_cached(cur_node.key);
                    // 当前move的tuple_id
                    move_tuple_id.insert(cur_node.key);
                }
                else if (move_has_neighbor(tmp_move))
                {
                    // continue to expand the clump until it is offload underneath the average level
                    MoveRecord<WorkloadType> new_move_rec;
                    get_most_co_accessed_neighbor(new_move_rec);
                    tmp_move.records.push_back(new_move_rec);

                    // after add new tuple, the dest may change
                    update_dest(tmp_move);

                    get_neighbor_cached(new_move_rec.record_key_);
                    move_tuple_id.insert(new_move_rec.record_key_);
                }
                else
                {
                    if (C_move.records.empty())
                    {
                        // something was wrong
                        return;
                    }
                    else
                    {
                        // start to move
                        //!TODO 先只move一次
                        moves.push_back(C_move);
                        cur_load += cost_delta_for_sender(C_move, overloaded_partition_id);
                        C_move.reset();
                        tmp_move.reset();
                        look_ahead = find_clump_look_ahead;
                        continue;
                    }
                }
                // if feasible continue to expand
                if(feasible(tmp_move, tmp_move.dest_partition_id)){
                    C_move = tmp_move;
                } else if(!C_move.records.empty()) {
                    // save current status as candidate, contiune to find better one 
                    look_ahead -= 1;
                }

                if(look_ahead == 0){
                    // fail to expand
                    moves.push_back(C_move);
                    cur_load += cost_delta_for_sender(C_move, overloaded_partition_id);
                    C_move.reset();
                    tmp_move.reset();
                    look_ahead = find_clump_look_ahead;
                    continue;
                }
            }
            return;
        }
        
        void reset(){
            big_node_heap.clear(); // <partition_id, big_heap>
            record_for_neighbor.clear();
            move_tuple_id.clear();
            hottest_tuple.clear(); // <key, frequency>
            load_partition.clear(); // <partition_id, load>
        }
        void update_dest(myMove<WorkloadType> &move)
        {
            /**
             * @brief 更新
             * 
             */
            if (!feasible(move, move.dest_partition_id))
            {
                // 不可行
                auto a = get_most_related_partition(move);
                DCHECK(a != -1);
                if (feasible(move, a))
                {
                    move.dest_partition_id = a;
                }
                else
                {
                    auto l = least_loaded_partition();
                    if (move.dest_partition_id != l && feasible(move, l))
                    {
                        move.dest_partition_id = l;
                    }
                }
            }
            // 不然就说明可行，还是d
        }

        void update_hottest_edge(const myTuple& cur_node){
            // big_heap
            auto cur_partition_heap = big_node_heap.find(cur_node.p_id);
            if(cur_partition_heap == big_node_heap.end()){
                // 
                fixed_priority_queue new_big_heap;
                new_big_heap.push_back(cur_node);
                big_node_heap.insert(std::make_pair(cur_node.p_id, new_big_heap));
            } else {
                cur_partition_heap->second.push_back(cur_node);
            }
        }
        void update_hottest_tuple(int32_t key_one){
            // hottest tuple
            auto cur_key = hottest_tuple.find(key_one);
            if(cur_key == hottest_tuple.end()){
                hottest_tuple.insert(std::make_pair(key_one, 1));
            } else {
                cur_key->second ++;
            }
        }
        void update_load_partition(const Node& cur_node){
            // partition load increase
            // 
            int32_t cur_weight = cur_node.on_same_coordi ? 1 : cross_txn_weight;
            // 
            auto cur_key_pd = load_partition.find(cur_node.from_p_id);
            if (cur_key_pd == load_partition.end()) {
                //
                load_partition.insert(std::make_pair(cur_node.from_p_id, cur_weight));
            } else {
                cur_key_pd->second += cur_weight;
            }

            cur_key_pd = load_partition.find(cur_node.to_p_id);
            if (cur_key_pd == load_partition.end()) {
                //
                load_partition.insert(std::make_pair(cur_node.to_p_id, cur_weight));
            } else {
                cur_key_pd->second += cur_weight;
            }
        }

    private:
        int32_t initial_dest_partition(const myMove<WorkloadType> &move) {
            // 
            int32_t min_delta;
            int32_t partition_id=-1;
            bool is_first = false;
            for(auto it = load_partition.begin(); it != load_partition.end(); it ++ ){
                int32_t cur_dest_pd = it->first;
                if(move.dest_partition_id == cur_dest_pd){
                    // 其他分区！
                    continue;
                }
                if(is_first == false){
                    min_delta = cost_delta_for_receiver(move, cur_dest_pd);
                    partition_id = cur_dest_pd;
                    is_first = true;
                } else {
                    int32_t delta = cost_delta_for_receiver(move, cur_dest_pd);
                    if(min_delta > delta){
                        min_delta = delta;
                        partition_id = cur_dest_pd;
                    }
                }
            }

            return partition_id;
        }
        void get_neighbor_cached(u_int64_t key) {
            /**
             * @brief get the neighbor of tuple [key] in degree DESC sequence
             * @param key description
             */
            if (record_for_neighbor.find(key) == record_for_neighbor.end())
            {
                //
                std::vector<std::pair<myKeyType, Node> > name_score_vec(record_degree[key].begin(), record_degree[key].end());
                std::sort(name_score_vec.begin(), name_score_vec.end(), 
                        [=](const std::pair<myKeyType, Node> &p1, const std::pair<myKeyType, Node> &p2)
                          {
                              return p1.second.degree > p2.second.degree;
                          });

                record_for_neighbor[key] = name_score_vec;
            }
        }
        int32_t get_most_related_partition(const myMove<WorkloadType> &move){
            /**
             * @brief 
             * 
             */
            std::map<int32_t, int32_t> partition_related;
            int32_t partition_id = -1;
            int32_t partition_degree = -1;

            for(auto it = move.records.begin(); it != move.records.end(); it ++ ){
                // 遍历每一条record
                auto key = it->record_key_;
                std::unordered_map<myKeyType, Node>& all_edges = record_degree[key];
                // 边权重
                for(auto itt = all_edges.begin(); itt != all_edges.end(); itt ++ ) {
                    Node& cur_n = itt->second;
                    DCHECK(it->src_partition_id == cur_n.from_p_id);

                    // int32_t cur_weight = 1;
                    // if(cur_n.to_p_id != cur_n.from_p_id){
                    //     cur_weight = cross_txn_weight;
                    // }

                    if(partition_related.find(cur_n.to_p_id) == partition_related.end()){
                        partition_related.insert(std::make_pair(cur_n.to_p_id, cur_n.degree )); // * cur_weight
                    } else {
                        partition_related[cur_n.to_p_id] += cur_n.degree ; // * cur_weight
                    }
                    // 更新最密切的
                    if(partition_related[cur_n.to_p_id] > partition_degree){
                        partition_degree = partition_related[cur_n.to_p_id];
                        partition_id = cur_n.to_p_id;
                    }
                }
            }

            return partition_id;
        }
        int32_t find_overloaded_partition(int32_t &average_load)
        {
            /**
             * @brief 根据平均值，找到overloaded的partition
             * 
             */
            int32_t partition_num = 0;

            std::vector<std::pair<int32_t, int32_t> > name_score_vec(load_partition.begin(), load_partition.end());
            std::sort(name_score_vec.begin(), name_score_vec.end(),
                      [=](const std::pair<int32_t, int32_t> &p1, const std::pair<int32_t, int32_t> &p2)
                      {
                          return p1.second > p2.second;
                      });

            for (auto it = load_partition.begin(); it != load_partition.end(); it++)
            {
                average_load += it->second;
                partition_num++;
            }
            average_load = average_load / partition_num;

            int32_t overloaded_partition_id = -1;

            for (auto it = name_score_vec.begin(); it != name_score_vec.end(); it++)
            {
                if (it->second > average_load)
                {
                    overloaded_partition_id = it->first;
                    break;
                }
            }

            return overloaded_partition_id;
        }

        void get_most_co_accessed_neighbor(MoveRecord<WorkloadType> &new_move_rec)
        {
            /**
             * @brief 找node中与 M 权重最大的边
             * 
             */
            Node node;
            node.degree = -1;

            for (auto it = record_for_neighbor.begin(); it != record_for_neighbor.end(); it++)
            {
                // 遍历每一条 move_rec
                const std::vector<std::pair<myKeyType, Node> > &cur = it->second;
                for (auto itt = cur.begin(); itt != cur.end(); itt++)
                {
                    // 遍历tuple的所有neighbor
                    const Node &cur_node = itt->second;
                    if(cur_node.degree <= node.degree)
                    {
                        break;
                    }
                    else if (move_tuple_id.find(cur_node.to) == move_tuple_id.end())
                    {
                        // 没有找到 因为cur 已经降序排过了，所以错了就直接退出
                        node = cur_node;
                        break;
                    }
                }
            }
            
            new_move_rec.set_real_key(node.to);
            new_move_rec.src_partition_id = node.to_p_id;

            return;
        }

        bool move_has_neighbor(const myMove<WorkloadType> &move)
        {
            /**
             * @brief find if current move has uncontained tuple
             * 
             */
            bool ret = false;
            for (auto it = move.records.begin(); it != move.records.end(); it++)
            {
                // 遍历每一条record
                const MoveRecord<WorkloadType> &cur_rec = *it;
                auto key = cur_rec.record_key_;

                std::vector<std::pair<myKeyType, Node> > &all_neighbors = record_for_neighbor[key];
                for (auto itt = all_neighbors.begin(); itt != all_neighbors.end(); itt++)
                {
                    if (move_tuple_id.find(itt->first) == move_tuple_id.end())
                    {
                        // 当前的值不在move里，说明还有neighbor
                        ret = true;
                        break;
                    }
                }
                if (ret)
                {
                    break;
                }
            }
            return ret;
        }

        bool feasible(const myMove<WorkloadType> &move, const int32_t &dest_partition_id)
        {
            //
            int32_t delta = cost_delta_for_receiver(move, dest_partition_id);
            int32_t dest_new_load = load_partition[dest_partition_id] + delta;
            bool  not_overload = ( dest_new_load < average_load );
            bool  minus_delta  = ( delta <= 0 );
            return not_overload || minus_delta;
            // true;
        }
        int32_t cost_delta_for_receiver(const myMove<WorkloadType> &move, const int32_t &dest_partition_id)
        {
            int32_t cost = 0;
            for(size_t i = 0 ; i < move.records.size(); i ++ ){
                const MoveRecord<WorkloadType>& cur = move.records[i]; 
                auto key = cur.record_key_;
                // 点权重
                cost += hottest_tuple[key];
                std::unordered_map<myKeyType, Node>& all_edges = record_degree[key];
                // 边权重
                for(auto it = all_edges.begin(); it != all_edges.end(); it ++ ) {
                    if(it->second.to_p_id == dest_partition_id){
                        cost -= it->second.degree; // * cross_txn_weight;
                    } else {
                        cost += it->second.degree; //  * cross_txn_weight;
                    }
                }
            }
            return cost;
        }

        int32_t cost_delta_for_sender(const myMove<WorkloadType> &move, const int32_t &src_partition_id)
        {
            /**
             * @brief 
             * @param src_partition_id 当前overload的partition
             */

            int32_t cost = 0;
            for(size_t i = 0 ; i < move.records.size(); i ++ ){
                const MoveRecord<WorkloadType>& cur = move.records[i]; 
                auto key = *(int32_t*)&cur.key;
                if(cur.src_partition_id == move.dest_partition_id){
                    // 没动？
                    continue;
                }
                // 点权重
                cost -= hottest_tuple[key];
                std::unordered_map<myKeyType, Node>& all_edges = record_degree[key];
                // 边权重
                for(auto it = all_edges.begin(); it != all_edges.end(); it ++ ) {
                    // 
                    if(it->second.from_p_id == src_partition_id){
                        cost -= it->second.degree;//  * cross_txn_weight;
                    } else {
                        cost += it->second.degree;// * cross_txn_weight;
                    }
                }
            }
            return cost;
        }

        int32_t least_loaded_partition(){
            /**
             * @brief 找当前负载最小的partition
             * 
             */
            int32_t partition_id;
            int32_t partition_degree;

            for(auto it = load_partition.begin(); it != load_partition.end(); it ++ ){
                if(it == load_partition.begin()){
                    partition_degree = it->second;
                    partition_id = it->first;
                }
                // 
                if(it->second < partition_degree){
                    partition_degree = it->second;
                    partition_id = it->first;
                }
            }
            return partition_id;
        }

        int32_t average_load;
        int32_t cur_load;
    public:
        std::unordered_map<int32_t, fixed_priority_queue> big_node_heap; // <partition_id, big_heap>
        std::unordered_map<myKeyType, std::vector<std::pair<myKeyType, Node> > > record_for_neighbor;
        std::unordered_set<myKeyType> move_tuple_id;
        std::unordered_map<myKeyType, int32_t> hottest_tuple; // <key, frequency>
        std::map<int32_t, int32_t> load_partition; // <partition_id, load>

        std::unordered_map<myKeyType, std::unordered_map<myKeyType, Node> >& record_degree; // unordered_ unordered_

    };
}