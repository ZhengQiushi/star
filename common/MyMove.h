#pragma once

#include "core/Context.h"
#include "benchmark/ycsb/Schema.h"
#include "core/Defs.h"

#include <vector>
// #include <unordered_map>
#include "common/HashMap.h"
#include "common/ShareQueue.h"
#include "common/skiplist/skip_list.h"
#include <unordered_set>
#include <queue>

#include <fstream>
#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <metis.h>
#include <map>
#include <limits.h>

#include <glog/logging.h>

#define TOP_SIZE 10000000
namespace star
{
    struct TPCCdebug {
  bool isRemote() {
    for (auto i = 0; i < O_OL_CNT; i++) {
      if (INFO[i].OL_SUPPLY_W_ID != W_ID) {
        return true;
      }
    }
    return false;
  }

  void unpack_transaction(const simpleTransaction& t){
    auto record_key = t.keys[2];
    this->W_ID = (record_key & RECORD_COUNT_W_ID_VALID) >>  RECORD_COUNT_W_ID_OFFSET;
    this->D_ID = (record_key & RECORD_COUNT_D_ID_VALID) >>  RECORD_COUNT_D_ID_OFFSET;
    DCHECK(this->D_ID >= 1);
    this->C_ID = (record_key & RECORD_COUNT_C_ID_VALID) >>  RECORD_COUNT_C_ID_OFFSET;
    DCHECK(this->C_ID >= 1);

    for(size_t i = 0 ; i < t.keys.size(); i ++ ){
      auto record_key = t.keys[i];
      if(i >= 3){
        INFO[i - 3].OL_I_ID = (record_key & RECORD_COUNT_OL_ID_VALID);
        INFO[i - 3].OL_SUPPLY_W_ID = 
          (record_key & RECORD_COUNT_W_ID_VALID) >>  RECORD_COUNT_W_ID_OFFSET;
      }
    }
  }
  void print(){
    LOG(INFO) << this->W_ID << " " << this->D_ID << " " << this->C_ID;
  }
  std::string print_str(){
    return std::to_string(this->W_ID) + " " + 
           std::to_string(this->D_ID) + " " + 
           std::to_string(this->C_ID) ;
  }

  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  int8_t O_OL_CNT;

  struct NewOrderQueryInfo {
    int32_t OL_I_ID;
    int32_t OL_SUPPLY_W_ID;
    int8_t OL_QUANTITY;
  };

  NewOrderQueryInfo INFO[15];
  std::vector<uint64_t> record_keys; // for migration
};

    #define MAX_COORDINATOR_NUM 80
    struct Clump {
        using T = u_int64_t;
    public:
        int hot;
        std::vector<simpleTransaction*> txns;
        std::unordered_map<T, int> keys;
        int dest;

        int move_cost[MAX_COORDINATOR_NUM] = {0};
        std::vector<std::vector<int>>& cost;

        Clump(simpleTransaction* txn, std::vector<std::vector<int>>& cost)
        : cost(cost){
            hot = 0;
            dest = -1;
            memset(move_cost, 0, sizeof(move_cost));
            
            AddTxn(txn);
        }
        // 
        bool CountTxn(simpleTransaction* txn){
            for(auto& i : txn->keys){
            if(keys.count(i)){
                return true;
            }
            }
            return false;
        }
        void AddTxn(simpleTransaction* txn){
            for(auto& i : txn->keys){
                keys[i] += 1;
            }
            auto& costs = cost[txn->idx_];

            int min_cost = INT_MAX;
            int idx = -1;

            for(size_t i = 0 ; i < costs.size(); i ++ ){
                move_cost[i] += costs[i];
                if(min_cost > move_cost[i]){
                    min_cost = move_cost[i];
                    idx = i;
                }
            }

            this->dest = idx;
            txns.push_back(txn);
            hot += 1;
        }

        std::pair<int, int> CalIdleNode(const std::unordered_map<size_t, int>& idle_node,
                                        bool is_init){
            int idle_coord_id = -1;                        
            int min_cost = INT_MAX;
            // int idle_coord_id = -1;
            for(auto& idle: idle_node){
                // 
                if(is_init){
                    if(min_cost > move_cost[idle.first]){
                        min_cost = move_cost[idle.first];
                        idle_coord_id = idle.first;
                    }
                } else {
                    if(move_cost[idle.first] <= -150){
                        idle_coord_id = idle.first;
                    }
                }
            }
            return std::make_pair(idle_coord_id, min_cost);
        }

        std::pair<int, int> CalIdleNodes(const std::unordered_map<size_t, int>& idle_node, 
                                         bool migrate_only){
            int idle_coord_id = -1;                        
            int min_cost = INT_MAX;
            // int idle_coord_id = -1;
            for(auto& idle: idle_node){
                int cur_min_cost = 0;
                for(size_t i = 0 ; i < txns.size(); i ++ ){
                    cur_min_cost += cost[txns[i]->idx_][idle.first];
                }
                if(min_cost > cur_min_cost && (migrate_only || true)){
                    min_cost = cur_min_cost;// move_cost[idle.first];
                    idle_coord_id = idle.first;
                }
            }
            return std::make_pair(idle_coord_id, min_cost);
        }

        void UpdateDest(int new_dest){
            move_cost[dest] -= this->hot;
            move_cost[new_dest] += this->hot;
            dest = new_dest;

            // std::string print = "";
            for(auto& t : txns){
                t->is_distributed = true;
                // add dest busy
                t->is_real_distributed = true;
                // t->keys

                // TPCCdebug debug;
                // debug.unpack_transaction(*t);
                // print += debug.print_str();
                // print += " -> " + std::to_string(t->destination_coordinator) + " " 
                //                 + std::to_string(new_dest) + "     ";

                t->destination_coordinator = new_dest;
                t->access_frequency = this->hot;
            }

            
            // LOG(INFO) << print;
        }

    };

    struct Clumps {
    public:
        std::vector<Clump> clumps;
        std::vector<std::vector<int>>& cost;

        std::unordered_map<uint64_t, int> key_clumps_idx;

        Clumps(std::vector<std::vector<int>>& cost):cost(cost){

        }
        void AddTxn(simpleTransaction* txn){
            bool need_new_clump = true;

            for(size_t i = 0; i < txn->keys.size(); i ++ ){
                if(!key_clumps_idx.count(txn->keys[i])) continue;
                // LOG(INFO) << "ADD TO " << key_clumps_idx[txn->keys[i]] << " " << txn->keys[0] << " " << txn->keys[1] << " " << txn->keys[4];
                need_new_clump = false;
                clumps[key_clumps_idx[txn->keys[i]]].AddTxn(txn);
                break;
            }
            if(need_new_clump){
                for(size_t i = 0; i < txn->keys.size(); i ++ ){
                    key_clumps_idx[txn->keys[i]] = clumps.size();
                }
                // LOG(INFO) << "ADD TO " << clumps.size() << " " << txn->keys[0] << " " << txn->keys[1] << " " << txn->keys[4];
                clumps.push_back(Clump(txn, cost));
            }
        }
        size_t Size(){
        return clumps.size();
        }
        Clump& At(int i){
        return clumps[i];
        }
        std::vector<int> Sort(){
            std::vector<int> ret;

            for(size_t i = 0 ; i < clumps.size(); i ++ ){
                ret.push_back(i);
            }
            std::sort(ret.begin(), ret.end(), [&](int a, int b){
                return clumps[a].hot < clumps[b].hot;
            });
            
            updateTxnHot();

            return ret;
        }
        void updateTxnHot(){
            for(auto& c : clumps){
                // std::string print = "";
                for(auto& t : c.txns){
                    t->access_frequency = c.hot;
                }
                // LOG(INFO) << print;
            }
        }
    };


    template <class Workload>
    struct MoveRecord{
        using myKeyType = uint64_t;
        using WorkloadType = Workload;

        int32_t table_id;
        int32_t key_size;
        int32_t field_size;
        int32_t src_coordinator_id;
        myKeyType record_key_;
        int32_t access_frequency;

        union key_ {
            key_(){
                // can't be default
            }
            ycsb::ycsb::key ycsb_key;
            tpcc::warehouse::key w_key;
            tpcc::district::key d_key;
            tpcc::customer::key c_key;
            tpcc::stock::key s_key;
        } key{};
        
        union val_ {
            val_(){
                ;
            }           
            star::ycsb::ycsb::value ycsb_val;
            tpcc::warehouse::value w_val;
            tpcc::district::value d_val;
            tpcc::customer::value c_val;
            tpcc::stock::value s_val;
        } value{};

        MoveRecord(){
            table_id = 0;
            // memset(&key, 0, sizeof(key));
            // memset(&value, 0, sizeof(value));
        }
        ~MoveRecord(){
            table_id = 0;
            // memset(&key, 0, sizeof(key));
            // memset(&value, 0, sizeof(value));
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

                switch (table_id)
                {
                case tpcc::warehouse::tableID:{
                    const auto &v = *static_cast<const tpcc::warehouse::value *>(record_val);  
                    this->value.w_val = v;
                    break;
                }
                case tpcc::district::tableID:{
                    const auto &v = *static_cast<const tpcc::district::value *>(record_val);  
                    this->value.d_val = v;
                    break;
                }
                case tpcc::customer::tableID:{
                    const auto &v = *static_cast<const tpcc::customer::value *>(record_val);  
                    this->value.c_val = v;
                    break;
                }
                case tpcc::stock::tableID:{
                    const auto &v = *static_cast<const tpcc::stock::value *>(record_val);  
                    this->value.s_val = v;
                    break;
                }
                default:
                    DCHECK(false);
                    break;
                }
            }
            return;

        }
        
        RouterValue* get_router_val(ITable* router_table){
            RouterValue* router_val;

            switch (this->table_id)
            {
            case tpcc::warehouse::tableID:{
                router_val = (RouterValue*)router_table->search_value((void*) &key.w_key);
                break;
            }
            case tpcc::district::tableID:{
                router_val = (RouterValue*)router_table->search_value((void*) &key.d_key);
                break;
            }
            case tpcc::customer::tableID:{
                router_val = (RouterValue*)router_table->search_value((void*) &key.c_key);
                break;
            }
            case tpcc::stock::tableID:{
                router_val = (RouterValue*)router_table->search_value((void*) &key.s_key);
                break;
            }
            default:
                DCHECK(false);
                break;
            }
            return router_val;
        }
        RouterValue* get_router_val(ImyRouterTable* router_table){
            RouterValue* router_val;
            DCHECK(false);
            return router_val;
        }
        void reset(){
            table_id = 0;
            key_size = 0;
            field_size = 0;
            src_coordinator_id = 0;
            record_key_ = 0;
            access_frequency = 0;
        }
    };

    template <class Workload>
    struct myMove
    {   
        using myKeyType = uint64_t;
    /**
     * @brief a bunch of record that needs to be transformed 
     * 
     */ 
        using WorkloadType = Workload;

        std::vector<MoveRecord<WorkloadType>> records;
        int32_t dest_coordinator_id;
        int32_t metis_dest_coordinator_id; // only for metis
        int32_t access_frequency;
        
        myMove(){
            reset(); 
        }
        void reset()
        {
            records.clear();
            dest_coordinator_id = -1;
            access_frequency = 0;
        }

        void copy(const std::shared_ptr<myMove<Workload>>& m_){
            records = m_->records;
            dest_coordinator_id = m_->dest_coordinator_id;
        }
    };

    static const int32_t cross_txn_weight = 50;
    static const int32_t find_clump_look_ahead = 5;

    struct Node
    {
        using myKeyType = uint64_t;
        myKeyType from, to;
        int32_t from_c_id, to_c_id;
        int degree;
        int on_same_coordi;
    };
    struct myTuple{
        using myKeyType = uint64_t;
        myKeyType key;
        int32_t c_id;
        int32_t degree;
        myTuple(myKeyType key_, int32_t c_id_, int32_t degree_){
            key = key_;
            c_id = c_id_;
            degree = degree_;
        }
        bool operator< (const myTuple& n2) const {    
            if(n2.degree == this->degree){
                return n2.key > this->key;
            } else {
                return  n2.degree > this->degree ;  //"<"为从大到小排列，">"为从小到大排列    
            }        
        }  
        bool operator> (const myTuple& n2) const {    
            if(n2.degree == this->degree){
                return n2.key > this->key;
            } else {
                return  n2.degree < this->degree ;  //"<"为从大到小排列，">"为从小到大排列    
            }
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

    typedef typename goodliffe::skip_list<myTuple, std::greater<myTuple>> my_skip_list;
    template<long long N>
    class top_frequency_key: public my_skip_list {
        public:
            // void push_back(const myTuple& tuple){
            //     if(freqency_key_list.size() > N) {
            //         // 
            //         freqency_key_list.erase(freqency_key_list.back());
            //     }
            //     if(look_up_cache.count(tuple.key)){
            //         // delete, O(1)
            //         freqency_key_list.erase(look_up_cache[tuple.key]);
            //         look_up_cache.erase(tuple.key);
            //     }
            //     // insert and update map, O(log(n))
            //     auto pos = freqency_key_list.insert(tuple);
            //     DCHECK(pos.second == true);
            //     look_up_cache[tuple.key] = pos.first;
            // }
            void push_back(const myTuple& tuple){
                if(size() > N) {
                    // 
                    erase(back());
                }
                // LOG(INFO) << "push " << tuple.key;
                // if(look_up_cache.count(tuple.key)){
                //     // delete, O(1)
                //     LOG(INFO) << "delete " << tuple.key;
                //     // erase(look_up_cache[tuple.key]);

                //     look_up_cache.erase(tuple.key);
                // }
                auto it = find(tuple);
                if(it != end()){
                    // LOG(INFO) << "delete " << tuple.key;
                    erase(it);
                }
                // insert and update map, O(log(n))
                auto pos = insert(tuple);
                DCHECK(pos.second == true);
                look_up_cache[tuple.key] = pos.first;
            }
        private:
            // my_skip_list freqency_key_list;
            std::unordered_map<uint64_t, my_skip_list::const_iterator> look_up_cache;
    };

    template <class Workload>
    class Clay
    {   
        // const std::size_t N = 10086;
        using WorkloadType = Workload;
        using DatabaseType = typename WorkloadType::DatabaseType;

        using myKeyType = uint64_t;
        using myValueType = std::unordered_map<myKeyType, Node>;
        
    public:
        Clay(const Context &context, DatabaseType& db, std::atomic<uint32_t>& worker_status):
            context(context), db(db), worker_status(worker_status), record_degree(0, 0){
                edge_nums = 0;
        }
        int64_t get_vertex_num(){
            return hottest_tuple.size();
        }
        int64_t get_edge_num(){
            return edge_nums;
        }
        void my_run_offline(std::string& src_file, std::string& dst_file, int start_ts, int end_ts){
            // LOG(INFO) << "start";
            LOG(INFO) << "history init done";
            auto start_time = std::chrono::steady_clock::now();
            init_with_history(context.data_src_path_dir + src_file, start_ts, end_ts - 1);
            LOG(INFO) << "distributed transations : " << distributed_edges;
            for(auto& i: distributed_edges_on_coord){
                LOG(INFO) << i.first << " : " << i.second;
            }

            auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();

            LOG(INFO) << "[done] init time: " << latency * 1.0 / 1000 << " s";

            // my_clay->metis_partition_graph("/home/star/data/resultss_partition_30_60.xls");
            my_find_clump(context.data_src_path_dir + dst_file);
            LOG(INFO) << "done";
            latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();

            LOG(INFO) << "[done] take time: " << latency * 1.0 / 1000 << " s";

            clear_graph();
        }

        void run_clay_offline(std::string& src_file, std::string& dst_file, int start_ts, int end_ts){
            
            LOG(INFO) << "history init done";

            auto start_time = std::chrono::steady_clock::now();
            init_with_history(context.data_src_path_dir + src_file, start_ts, end_ts);
            auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();
            LOG(INFO) << "distributed transations : " << distributed_edges;
            for(auto& i: distributed_edges_on_coord){
                LOG(INFO) << i.first << " : " << i.second;
            }
            LOG(INFO) << "  load : ";
            for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                LOG(INFO) << i << " : " << node_load[i];
            }
            LOG(INFO) << "[done] init time: " << latency * 1.0 / 1000 << " s";


            start_time = std::chrono::steady_clock::now();
            find_clump();
            implement_clump();
            latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();
            LOG(INFO) << "[done] take time: " << latency * 1.0 / 1000 << " s";
            save_clay_moves(context.data_src_path_dir + dst_file + "_0");
            
            LOG(INFO) << "first round done";
            clear_graph();

            start_time = std::chrono::steady_clock::now();
            init_with_history(context.data_src_path_dir + src_file, start_ts, end_ts);
            LOG(INFO) << "distributed transations : " << distributed_edges;
            for(auto& i: distributed_edges_on_coord){
                LOG(INFO) << i.first << " : " << i.second;
            }
            LOG(INFO) << "  load : ";
            for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                LOG(INFO) << i << " : " << node_load[i];
            }
            latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();
            LOG(INFO) << "[done] init time: " << latency * 1.0 / 1000 << " s";

            start_time = std::chrono::steady_clock::now();
            find_clump();
            implement_clump();
            
            latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - start_time)
                                        .count();

            LOG(INFO) << "[done] take time: " << latency * 1.0 / 1000 << " s";
            save_clay_moves(context.data_src_path_dir + dst_file + "_1");
            clear_graph();
        }

        void start_runtime(){
            // main loop
            ExecutorStatus status;
            size_t batch_size = 50;
            for (;;) {
                static int cnt = 0;
                status = static_cast<ExecutorStatus>(worker_status.load());
                do {
                    status = static_cast<ExecutorStatus>(worker_status.load());
                    if (status == ExecutorStatus::EXIT) {
                        LOG(INFO) << "Clay " << " exits.";
                        return;
                    }
                    bool success = false;
                    std::shared_ptr<simpleTransaction> txn = transactions_queue.pop_no_wait(success);
                    
                    if(success){
                        //
//                        LOG(INFO) << "num : " << (cnt + 1) % batch_size << "Capture: " << txn->keys[0] << " " << txn->keys[1];
                        update_record_degree_set(txn->keys);
                        cnt ++ ;
                    } else {
                        std::this_thread::sleep_for(std::chrono::microseconds(5));
                    }
                } while(status != ExecutorStatus::CLEANUP && (cnt + 1) % batch_size != 0);
                // 
                // std::vector<myMove<WorkloadType>> tmp_moves;
                if(movable_flag.load() == false){
                    find_clump(); // tmp_moves);
                    // moves_last_round = tmp_moves;
                    if(move_plans.size() > 0){
                        movable_flag.store(true);
                    }
                    transactions_queue.clear();
                    clear_graph();
                    node_load.clear();
                }

                // if( (cnt + 1) % (batch_size * 10) != 0){
                    
                // }

                
            }
            // not end here!
        }


        



        void start(){
            DCHECK(false);
        }


        bool push_txn(std::shared_ptr<simpleTransaction> txn){
            return transactions_queue.push_no_wait(txn);
        }
        size_t get_file_size(const char* file_name) {
            struct stat st;
            stat(file_name, &st);
            return st.st_size;
        }

        // /*使用mmap进行文件映射*/
        size_t read_file_from_mmap(const char* file_name, char** mem_data) {
            size_t file_size = get_file_size(file_name);
            LOG(INFO) << "READ FILE SIZE : " << file_size;
            //Open file
            int fd = open(file_name, O_RDONLY, 0);
            if (fd == -1){
                LOG(INFO) << "Error open file for read";
                return file_size;
            }
            //Execute mmap
            void* mmapped_data = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
            if (mmapped_data == MAP_FAILED) {
                close(fd);
                LOG(INFO) << "Error mmapping the file";
                return file_size;
            }

            //Write the mmapped data buffer
            /*将其内容拷贝到内存缓冲区*/
            *mem_data = new char[file_size];
            // metis_file_read_ptr_ = init_metis_file_;

            memcpy(*mem_data, mmapped_data, file_size);
            
            //Cleanup
            int rc = munmap(mmapped_data, file_size);
            if (rc != 0) {
                close(fd);
                LOG(INFO) << "Error un-mmapping the file";
                return file_size;
            }
        //    cout << ret << endl;
            close(fd);

            LOG(INFO) << "READ FILE DONE ";
            
            return file_size;
        }
        void reset_init_file(){
            init_metis_file_ = nullptr;
        }
        void init_with_history(const std::string& workload_path, int start_timestamp, double end_timestamp){
            // int num_samples = 250000;
            // std::string input_path = "/home/star/data/result_test.xls";
            // std::string output_path = "/home/star/data/my_graph";
            int file_row_cnt_ = 0;
            int txn_sz = 10;
            if(WorkloadType::which_workload == myTestSet::TPCC){
                txn_sz = 13;
            }

            if(init_metis_file_ == nullptr){
                file_size_ = read_file_from_mmap(workload_path.c_str(), &init_metis_file_);
                metis_file_read_ptr_ = init_metis_file_;
                if(metis_file_read_ptr_ == nullptr) {
                    return;
                }
            }
            LOG(INFO) << "START READ INTO GRAPH";

            std::string _line = "";
            std::vector<uint64_t> keys;

            int cur_row_cnt = 0;
            while (metis_file_read_ptr_ != nullptr && metis_file_read_ptr_ - init_metis_file_ < file_size_){
                //解析每行的数据
                bool is_in_range_ = true;
                bool is_break = false;

                char* line_end_ = strchr(metis_file_read_ptr_, '\n');
                if(line_end_ == nullptr){
                    break;
                }
                if(file_row_cnt_ > 0){
                    keys.clear();
                    int col_cnt_ = 0;
                    char *per_key_ = strtok_r(metis_file_read_ptr_, "\t", &saveptr_);
                    while(per_key_ != NULL){
                        if(col_cnt_ == 0){
                            double ts_ = atof(per_key_);
                            if(ts_ > end_timestamp){
                                is_break = true;
                            }
                            if(ts_ < start_timestamp || ts_ > end_timestamp){
                                is_in_range_ = false;
                                break;
                            } 
                            is_in_range_ = true;
                        } else {
                            char * pEnd;
                            uint64_t key_ = strtoull(per_key_, &pEnd, 10);
                            keys.push_back(key_);
                        }
                        col_cnt_ ++ ;
                        if(col_cnt_ > txn_sz){
                            break;
                        }
                        per_key_ = strtok_r(NULL, "\t", &saveptr_);
                    }

                    if(is_in_range_ && keys.size() > 0){
                        update_record_degree_set(keys);
                        cur_row_cnt ++ ;
                    }
                }
                
                file_row_cnt_ ++ ;
                // if(file_row_cnt_ > 2){ // debug
                //     break;
                // }
                
                metis_file_read_ptr_ = line_end_ + 1;
                if(metis_file_read_ptr_ == NULL){
                    LOG(INFO) << "WHY";
                }
                if(is_break == true){
                    break;
                }
            }
            LOG(INFO) << "file_row_cnt_ : " << cur_row_cnt << " " << file_row_cnt_;
        }

        void metis_partition_graph(const std::string& output_path_){
            /**
             * @brief 
             * 
             */
            int vexnum = get_vertex_num();
            int edgenum = get_edge_num();

            std::vector<idx_t> xadj(0);   // 压缩邻接矩阵
            std::vector<idx_t> adjncy(0); // 压缩图表示
            std::vector<idx_t> adjwgt(0); // 边权重
            std::vector<idx_t> vwgt(0);   // 点权重
            

            std::unordered_map<uint64_t, int32_t> key_coordinator_cache;

            for(size_t i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){                
                uint64_t key = hottest_tuple_index_seq[i];

                xadj.push_back(adjncy.size()); // 
                vwgt.push_back(hottest_tuple[key]);

                myValueType* it = (myValueType*)record_degree.search_value(&key);
                
                for(auto edge: *it){
                    adjncy.push_back(hottest_tuple_index_[edge.first]); // 节点id从0开始
                    adjwgt.push_back(edge.second.degree);

                    // cache coordinator
                    key_coordinator_cache[edge.second.from] = edge.second.from_c_id;
                    key_coordinator_cache[edge.second.to] = edge.second.to_c_id;
                }
            }
            xadj.push_back(adjncy.size());
            
            idx_t nVertices = xadj.size() - 1;      // 节点数
            idx_t nWeights = 1;                     // 节点权重维数
            idx_t nParts = key_coordinator_cache.size() / 50;   // 子图个数≥2
            idx_t objval;                           // 目标函数值
            std::vector<idx_t> parts(nVertices, 0); // 划分结果
            std::cout << key_coordinator_cache.size() << " " << key_coordinator_cache.size() / 50 << std::endl;
            int ret = METIS_PartGraphKway(&nVertices, &nWeights, xadj.data(), adjncy.data(),
                vwgt.data(), NULL, adjwgt.data(), &nParts, NULL,
                NULL, NULL, &objval, parts.data());

            if (ret != rstatus_et::METIS_OK) { 
                std::cout << "METIS_ERROR" << std::endl; 
            }

            // print for logs
            std::cout << "METIS_OK" << std::endl;
            std::cout << "objval: " << objval << std::endl;
            
            std::vector<std::shared_ptr<myMove<WorkloadType>>> metis_move(nParts);
            for(size_t j = 0; j < nParts; j ++ ){
                metis_move[j] = std::make_shared<myMove<WorkloadType>>();
                metis_move[j]->metis_dest_coordinator_id = j;
            }

            for(size_t i = 0 ; i < parts.size(); i ++ ){
                uint64_t key_ = hottest_tuple_index_seq[i];
                int32_t source_c_id = key_coordinator_cache[key_];
                int32_t dest_c_id = parts[i];

                MoveRecord<WorkloadType> new_move_rec;
                new_move_rec.set_real_key(key_);

                metis_move[dest_c_id]->records.push_back(new_move_rec);
            }


            std::ofstream outpartition(output_path_);
            for (size_t i = 0; i < parts.size(); i++) { 
            }
            
            for(size_t j = 0; j < metis_move.size(); j ++ ){
                if(metis_move[j]->records.size() > 0){
                    outpartition << j << "\t";
                    for(int i = 0 ; i < metis_move[j]->records.size(); i ++ ){
                        outpartition << metis_move[j]->records[i].record_key_ << "\t";
                    }
                    outpartition << "\n";
                    move_plans.push_no_wait(metis_move[j]);
                }
            }
            outpartition.close();

            movable_flag.store(false);
        }
        

         void metis_partiion_read_from_file(const std::string& partition_filename, int batch_size, ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& cur_move_plans){
            /***
             * 
            */
           int file_row_cnt_ = 0;
            // generate move plans
            // myMove [source][destination]
            if(batch_size == -1){
                if(init_partition_file_ != nullptr){
                    delete init_partition_file_;
                    init_partition_file_ = nullptr;
                }
                partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
                if(init_partition_file_ == nullptr){
                    LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                    return ;
                }
                partition_file_read_ptr_ = init_partition_file_;
            } else {
                if(init_partition_file_ == nullptr){
                    partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
                    if(init_partition_file_ == nullptr){
                        LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                        return ;
                    }
                    partition_file_read_ptr_ = init_partition_file_;
                }
            }

            


            std::size_t nParts = context.coordinator_num * 1000;
            // std::vector<std::shared_ptr<myMove<WorkloadType>>> metis_move(nParts);
            // metis_move.clear();
            // metis_move.resize(nParts);
            
            // for(size_t j = 0; j < nParts; j ++ ){
            //     metis_move[j] = std::make_shared<myMove<WorkloadType>>();
            //     metis_move[j]->metis_dest_coordinator_id = j;
            // }

            

            while (partition_file_read_ptr_ != nullptr && partition_file_read_ptr_ - init_partition_file_ < partition_file_size_){
                //解析每行的数据
                bool is_in_range_ = true;
                bool is_break = false;

                char* line_end_ = strchr(partition_file_read_ptr_, '\n');
                size_t len_ = line_end_ - partition_file_read_ptr_ + 1;
                char* tmp_line_ = new char[len_];
                memset(tmp_line_, 0, len_);
                memcpy(tmp_line_, partition_file_read_ptr_, len_ - 1);


                if(line_end_ == nullptr){
                    break;
                }
                
                int col_cnt_ = 0;
                // int row_id = 0;
                int access_frequency = 0;
                            char * pEnd;
                            
                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                char *per_key_ = strtok_r(tmp_line_, "\t", &saveptr_);
                while(per_key_ != NULL){
                    if(col_cnt_ == 0){
                        // row_id = atoll(per_key_);
                    } else if(col_cnt_ == 1){
                        metis_move->access_frequency = strtoull(per_key_, &pEnd, 10);
                    } else {
                        uint64_t key_ = strtoull(per_key_, &pEnd, 10);

                        MoveRecord<WorkloadType> new_move_rec;
                        new_move_rec.set_real_key(key_);

                        metis_move->records.push_back(new_move_rec);
                    } 
                    col_cnt_ ++ ;
                    per_key_ = strtok_r(NULL, "\t", &saveptr_);
                }
                
                if(metis_move->records.size() > 0){
                    cur_move_plans.push_no_wait(metis_move);
                    file_row_cnt_ ++ ;
                    partition_file_read_ptr_ = line_end_ + 1;
                    if(file_row_cnt_ > batch_size){
                        return ;
                    }
                }
                

            }

            LOG(INFO) << "file_row_cnt_ : " << " " << file_row_cnt_;

            movable_flag.store(false);

            return ;
        }

        void metis_partiion_read_from_file(const std::string& partition_filename){
            /***
             * 
            */
           int file_row_cnt_ = 0;
            // generate move plans
            // myMove [source][destination]

            if(init_partition_file_ != nullptr){
                delete init_partition_file_;
                init_partition_file_ = nullptr;
            }
            partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
            if(init_partition_file_ == nullptr){
                LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                return;
            }
            partition_file_read_ptr_ = init_partition_file_;


            std::size_t nParts = context.coordinator_num * 1000;
            // std::vector<std::shared_ptr<myMove<WorkloadType>>> metis_move(nParts);
            // metis_move.clear();
            // metis_move.resize(nParts);
            
            // for(size_t j = 0; j < nParts; j ++ ){
            //     metis_move[j] = std::make_shared<myMove<WorkloadType>>();
            //     metis_move[j]->metis_dest_coordinator_id = j;
            // }

            

            while (partition_file_read_ptr_ != nullptr && partition_file_read_ptr_ - init_partition_file_ < partition_file_size_){
                //解析每行的数据
                bool is_in_range_ = true;
                bool is_break = false;

                char* line_end_ = strchr(partition_file_read_ptr_, '\n');
                size_t len_ = line_end_ - partition_file_read_ptr_ + 1;
                char* tmp_line_ = new char[len_];
                memset(tmp_line_, 0, len_);
                memcpy(tmp_line_, partition_file_read_ptr_, len_ - 1);


                if(line_end_ == nullptr){
                    break;
                }
                
                int col_cnt_ = 0;
                // int row_id = 0;
                int access_frequency = 0;
                            char * pEnd;
                            
                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                char *per_key_ = strtok_r(tmp_line_, "\t", &saveptr_);
                while(per_key_ != NULL){
                    if(col_cnt_ == 0){
                        // row_id = atoll(per_key_);
                    } else if(col_cnt_ == 1){
                        metis_move->access_frequency = strtoull(per_key_, &pEnd, 10);
                    } else {
                        uint64_t key_ = strtoull(per_key_, &pEnd, 10);

                        MoveRecord<WorkloadType> new_move_rec;
                        new_move_rec.set_real_key(key_);

                        metis_move->records.push_back(new_move_rec);
                    } 
                    col_cnt_ ++ ;
                    per_key_ = strtok_r(NULL, "\t", &saveptr_);
                }
                
                if(metis_move->records.size() > 0){
                    move_plans.push_no_wait(metis_move);
                }
                
                file_row_cnt_ ++ ;
                partition_file_read_ptr_ = line_end_ + 1;
            }

            LOG(INFO) << "file_row_cnt_ : " << " " << file_row_cnt_;

            movable_flag.store(false);

            return;
        }


        void clay_partiion_read_from_file(const std::string& partition_filename, int batch_size, ShareQueue<std::shared_ptr<myMove<WorkloadType>>>& cur_move_plans){
            /***
             * 
            */
            // generate move plans
            // myMove [source][destination]

           int file_row_cnt_ = 0;
            // generate move plans
            // myMove [source][destination]
            if(batch_size == -1){
                if(init_partition_file_ != nullptr){
                    delete init_partition_file_;
                    init_partition_file_ = nullptr;
                }
                partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
                if(init_partition_file_ == nullptr){
                    LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                    return ;
                }
                partition_file_read_ptr_ = init_partition_file_;
            } else {
                if(init_partition_file_ == nullptr){
                    partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
                    if(init_partition_file_ == nullptr){
                        LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                        return ;
                    }
                    partition_file_read_ptr_ = init_partition_file_;
                }
            }

            while (partition_file_read_ptr_ != nullptr && partition_file_read_ptr_ - init_partition_file_ < partition_file_size_){
                //解析每行的数据
                bool is_in_range_ = true;
                bool is_break = false;

                char* line_end_ = strchr(partition_file_read_ptr_, '\n');
                size_t len_ = line_end_ - partition_file_read_ptr_ + 1;
                char* tmp_line_ = new char[len_];
                memset(tmp_line_, 0, len_);
                memcpy(tmp_line_, partition_file_read_ptr_, len_ - 1);


                if(line_end_ == nullptr){
                    break;
                }
                
                int col_cnt_ = 0;
                // int row_id = 0;
                int access_frequency = 0;
                char* pEnd;

                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                char *per_key_ = strtok_r(tmp_line_, "\t", &saveptr_);
                while(per_key_ != NULL){
                    if(col_cnt_ == 0){
                        // row_id = atoll(per_key_);
                    } else if(col_cnt_ == 1){
                        metis_move->dest_coordinator_id = strtoull(per_key_, &pEnd, 10);;
                    } else {
                        uint64_t key_ = strtoull(per_key_, &pEnd, 10);;

                        MoveRecord<WorkloadType> new_move_rec;
                        new_move_rec.set_real_key(key_);

                        metis_move->records.push_back(new_move_rec);
                    } 
                    col_cnt_ ++ ;
                    per_key_ = strtok_r(NULL, "\t", &saveptr_);
                }
                
                if(metis_move->records.size() > 0){
                    cur_move_plans.push_no_wait(metis_move);
                    file_row_cnt_ ++ ;
                    partition_file_read_ptr_ = line_end_ + 1;
                    if(file_row_cnt_ > batch_size){
                        return ;
                    }
                }
                

            }

            LOG(INFO) << "file_row_cnt_ : " << " " << file_row_cnt_;

            movable_flag.store(true);

            return ;
        }


        void clay_partiion_read_from_file(const std::string& partition_filename){
            /***
             * 
            */
            // generate move plans
            // myMove [source][destination]
            int file_row_cnt_ = 0;
            if(init_partition_file_ != nullptr){
                delete init_partition_file_;
                init_partition_file_ = nullptr;
            }
            partition_file_size_ = read_file_from_mmap(partition_filename.c_str(), &init_partition_file_);
            if(init_partition_file_ == nullptr){
                LOG(ERROR) << "FAILED TO DO read_file_from_mmap FROM " << partition_filename;
                return;
            }
            partition_file_read_ptr_ = init_partition_file_;

            while (partition_file_read_ptr_ != nullptr && partition_file_read_ptr_ - init_partition_file_ < partition_file_size_){
                //解析每行的数据
                bool is_in_range_ = true;
                bool is_break = false;

                char* line_end_ = strchr(partition_file_read_ptr_, '\n');
                size_t len_ = line_end_ - partition_file_read_ptr_ + 1;
                char* tmp_line_ = new char[len_];
                memset(tmp_line_, 0, len_);
                memcpy(tmp_line_, partition_file_read_ptr_, len_ - 1);


                if(line_end_ == nullptr){
                    break;
                }
                
                int col_cnt_ = 0;
                // int row_id = 0;
                int access_frequency = 0;
                char* pEnd;

                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                char *per_key_ = strtok_r(tmp_line_, "\t", &saveptr_);
                while(per_key_ != NULL){
                    if(col_cnt_ == 0){
                        // row_id = atoll(per_key_);
                    } else if(col_cnt_ == 1){
                        metis_move->dest_coordinator_id = strtoull(per_key_, &pEnd, 10);;
                    } else {
                        uint64_t key_ = strtoull(per_key_, &pEnd, 10);;

                        MoveRecord<WorkloadType> new_move_rec;
                        new_move_rec.set_real_key(key_);

                        metis_move->records.push_back(new_move_rec);
                    } 
                    col_cnt_ ++ ;
                    per_key_ = strtok_r(NULL, "\t", &saveptr_);
                }
                
                if(metis_move->records.size() > 0){
                    move_plans.push_no_wait(metis_move);
                }
                
                file_row_cnt_ ++ ;
                partition_file_read_ptr_ = line_end_ + 1;
            }

            LOG(INFO) << "file_row_cnt_ : " << " " << file_row_cnt_;

            movable_flag.store(true);

            return;
        }


        
        void trace_graph(const std::string& path){

            std::ofstream outfile_excel_index;
            outfile_excel_index.open(path+".index", std::ios::trunc); // ios::trunc


            std::ofstream outfile_excel;
            outfile_excel.open(path, std::ios::trunc); // ios::trunc
            
            // uint64_t key = context.partition_num * context.keysPerPartition;
            int64_t vertex_num = hottest_tuple.size();

            outfile_excel << vertex_num << " " << edge_nums << "                        \n";

            for(int i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){
                uint64_t key = hottest_tuple_index_seq[i];
                uint64_t key_index = i + 1;

                myValueType* it = (myValueType*)record_degree.search_value(&key);
                
                outfile_excel_index << key << "->" << key_index << "\n";

                outfile_excel << hottest_tuple[key]; // vertex weight
                for(auto edge: *it){
                    outfile_excel << " " << hottest_tuple_index_[edge.first] << " " << edge.second.degree;
                }
                outfile_excel << "\n";
            }
            outfile_excel.close();
            outfile_excel_index.close();
        }

        int find_clump_main(int32_t overloaded_coordinator_id){
            int new_plans_cnt = 0;

            if (big_node_heap.find(overloaded_coordinator_id) == big_node_heap.end()){
                return new_plans_cnt;
            }

            auto& cur_load = node_load[overloaded_coordinator_id];

            //
            std::shared_ptr<myMove<WorkloadType>> C_move(new myMove<WorkloadType>()); 
            std::shared_ptr<myMove<WorkloadType>> tmp_move(new myMove<WorkloadType>());
            
            // 
            auto tuple_ptr = big_node_heap[overloaded_coordinator_id].begin();
            int big_node_size = big_node_heap[overloaded_coordinator_id].size();
            int cur_node_used_size = 0;

            // LOG(INFO) << "BEFORE SIZE: " << big_node_heap[overloaded_coordinator_id].size();

            int32_t look_ahead = find_clump_look_ahead;

            // the cur_load will change as the clump keeps expand
            while (cur_load > average_load){
                
                if (tmp_move->records.empty()){
                    // hottest tuple
                    
                    bool new_rec = false;
                    while(cur_node_used_size < big_node_size) {
                        if(!move_tuple_id.count(tuple_ptr.get_node()->value.key)){
                            // have not moved yet
                            new_rec = true;
                            break;
                        }
                        auto old_tuple_ptr = tuple_ptr;
                        tuple_ptr++;
                        cur_node_used_size ++ ;
                        big_node_heap[overloaded_coordinator_id].erase(old_tuple_ptr);
                    }

                    if(!new_rec){
                        // all heap has been used 
                        break;
                    }
                    const myTuple* cur_node = &*tuple_ptr;

                    DCHECK(cur_node->c_id == overloaded_coordinator_id);
                    new_move_rec.reset();
                    new_move_rec.set_real_key(cur_node->key);
                    new_move_rec.src_coordinator_id = cur_node->c_id;

                    tmp_move->records.emplace_back(new_move_rec);
                    tmp_move->dest_coordinator_id = overloaded_coordinator_id;
                    // dest-partition
                    tmp_move->dest_coordinator_id = initial_dest_partition(tmp_move);// tmp_move->dest_coordinator_id = rec.src_coordinator_id == cur_node.from_c_id ? cur_node.to_c_id : cur_node.from_c_id;
                    DCHECK(tmp_move->dest_coordinator_id != -1);

                    // get its neighbor cached
                    get_neighbor_cached(cur_node->key);
                    // 当前move的tuple_id
                    
                }
                else if (!q_.empty())// move_has_neighbor(tmp_move))
                {
                    // continue to expand the clump until it is offload underneath the average level
                    auto node = q_.top();
                    q_.pop();
                    
                    new_move_rec.reset();
                    new_move_rec.set_real_key(node.to);
                    new_move_rec.src_coordinator_id = node.to_c_id;
                    tmp_move->records.push_back(new_move_rec);

                    // after add new tuple, the dest may change
                    update_dest(tmp_move);

                    get_neighbor_cached(new_move_rec.record_key_);
                }
                else
                {
                    if (C_move->records.empty()){
                        // something was wrong
                        break;
                    }
                    else{
                        // start to move
                        //!TODO 先只move一次
                        move_plans.push_no_wait(C_move);
                        new_plans_cnt ++ ;
                        cur_load += cost_delta_for_sender(C_move, overloaded_coordinator_id);
                        auto& dest_load = node_load[C_move->dest_coordinator_id];
                        dest_load += cost_delta_for_receiver(C_move, C_move->dest_coordinator_id);

                        C_move.reset(new myMove<WorkloadType>());
                        tmp_move.reset(new myMove<WorkloadType>());
                        look_ahead = find_clump_look_ahead;
                        continue;
                    }
                }

                // if feasible continue to expand
                if(feasible(tmp_move, tmp_move->dest_coordinator_id)){
                    C_move->copy(tmp_move);
                } else if(!C_move->records.empty()) {
                    // save current status as candidate, contiune to find better one 
                    look_ahead -= 1;
                }

                if(look_ahead == 0){
                    // fail to expand
                    move_plans.push_no_wait(C_move); // 
                    new_plans_cnt ++ ;

                    cur_load += cost_delta_for_sender(C_move, overloaded_coordinator_id);
                    auto& dest_load = node_load[C_move->dest_coordinator_id];
                    dest_load += cost_delta_for_receiver(C_move, C_move->dest_coordinator_id);

                    C_move.reset(new myMove<WorkloadType>());
                    tmp_move.reset(new myMove<WorkloadType>());
                    look_ahead = find_clump_look_ahead;
                    continue;
                }
            }

            // LOG(INFO) << "AFTER SIZE: " << big_node_heap[overloaded_coordinator_id].size();

            return new_plans_cnt;
        }

        long long cal_load_distribute(long long aver_val, 
                                const std::map<int32_t, long long>& busy_){
            long long cur_val = 0;
            for(auto& i : busy_){
            // 
            cur_val += (aver_val - i.second) * (aver_val - i.second);
            }
            cur_val /= busy_.size();

            return cur_val;
        }

        void implement_clump(){

            int size_ = move_plans.size();
            LOG(INFO) << " this round: " << size_;
            while(size_ > 0){
                size_ -- ;
                bool success = true;
                auto move_step = move_plans.pop_no_wait(success);

                if(!success){
                    LOG(INFO) << " fail to pop ";
                    break;
                }

                // implement
                for(auto& i : move_step->records){
                    if(WorkloadType::which_workload == myTestSet::YCSB){
                        int table_id = ycsb::ycsb::tableID;
                        auto router_table = db.find_router_table(table_id); // , coordinator_id_old);

                        auto router_val = (RouterValue*)router_table->search_value((void*) &i.record_key_);
                        router_val->set_dynamic_coordinator_id(move_step->dest_coordinator_id);
                    } else {
                        MoveRecord<WorkloadType> rec;
                        rec.set_real_key(i.record_key_);
                        auto router_table = db.find_router_table(rec.table_id); // , 
                        auto router_val = rec.get_router_val(router_table);
                        router_val->set_dynamic_coordinator_id(move_step->dest_coordinator_id);
                    }

                }
                // LOG(INFO) << " implement_clump : " << move_step->records[0].record_key_ << " " << move_step->records[1].record_key_ << " -> " << move_step->dest_coordinator_id;
                total_move_plans.push_no_wait(move_step);
            }
            
            for(size_t i = 0 ; i < context.coordinator_num; i ++ ){
                LOG(INFO) << i << " : " << node_load[i];
            }
            
        }




        void find_clump() // std::vector<myMove<WorkloadType>> &moves
        {
            /***
             * @brief find all the records to move
             *        each moves is the set of records which may from different 
             *        partition but will be transformed to the same destination
            */
            std::lock_guard<std::mutex> l(mm);
            
            
            for(int i = 0 ; i < 10000; i ++ ) {
            // for(auto& i : node_load){
                average_load = 0;
                int32_t overloaded_coordinator_id = find_overloaded_node(average_load);
                if(overloaded_coordinator_id == -1){
                    return;
                }
                // 方差
                // long long cur_val = cal_load_distribute(average_load, node_load);
                // long long threshold = 100 / (context.coordinator_num / 2) * 100 / (context.coordinator_num / 2); // 2200 - 2800
                // if(cur_val < threshold){
                //     break;
                // }

                // check if there is the hottest tuple on this partition
                // while(true){
                int new_plans_cnt = find_clump_main(overloaded_coordinator_id);
                
                if(new_plans_cnt == 0){
                    // break;
                    // LOG(INFO) << "overloaded_coordinator_id : " << overloaded_coordinator_id << " DONE ";
                }
                // }
            }


            return;
        }


        void my_find_clump(std::string output_path_) // std::vector<myMove<WorkloadType>> &moves
        {
            /***
             * @brief find all the records to move
             *        each moves is the set of records which may from different 
             *        partition but will be transformed to the same destination
            */
            std::unordered_set<uint64_t> used;
            std::vector<std::shared_ptr<myMove<WorkloadType>>> metis_moves;
            

            for(size_t i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){                
                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                uint64_t key = hottest_tuple_index_seq[i];                
                // xadj.push_back(adjncy.size()); // 
                // vwgt.push_back(hottest_tuple[key]);

                std::queue<uint64_t> q_;
                q_.push(key);

                while(!q_.empty()){
                    uint64_t c_key = q_.front();
                    q_.pop();
                    if(used.count(c_key)){
                        continue;
                    }
                    used.insert(c_key);
                    MoveRecord<WorkloadType> new_move_rec;
                    new_move_rec.set_real_key(c_key);
                    new_move_rec.access_frequency = hottest_tuple[c_key];

                    metis_move->records.push_back(new_move_rec);

                    metis_move->access_frequency += new_move_rec.access_frequency;

                    myValueType* it = (myValueType*)record_degree.search_value(&c_key);
                    for(auto edge: *it){
                        // adjncy.push_back(hottest_tuple_index_[edge.first]); // 节点id从0开始
                        // adjwgt.push_back(edge.second.degree);
                        if(used.count(edge.second.to)){
                            continue;
                        }
                        // if(c_key == 4573){
                        //     LOG(INFO) << "wooo";
                        // }
                        // if(metis_move->records.size() > 15){
                        //     LOG(INFO) << edge.second.degree;
                        // }
                        // if(edge.second.degree < 50 * 20){
                        //     continue;
                        // }
                        q_.push(edge.second.to);
                    }

                }

                if(metis_move->records.size() > 1){
                    metis_moves.push_back(metis_move);
                }
            }


            // 
            std::ofstream outpartition(output_path_);
            
            for(size_t j = 0; j < metis_moves.size(); j ++ ){
                if(metis_moves[j]->records.size() > 0){
                    outpartition << j << "\t"; // id
                    outpartition << metis_moves[j]->access_frequency << "\t"; // weight

                    for(size_t i = 0 ; i < metis_moves[j]->records.size(); i ++ ){
                        outpartition << metis_moves[j]->records[i].record_key_ << "\t";
                    }
                    outpartition << "\n";
                    move_plans.push_no_wait(metis_moves[j]);
                }
            }
            outpartition.close();

            return;
        }

        void save_clay_moves(std::string output_path_){
            // 
            std::ofstream outpartition(output_path_);
            size_t move_plans_num = total_move_plans.size();
            int j = 0;
            while(move_plans_num > 0){
                bool success = false;
                std::shared_ptr<myMove<WorkloadType>> cur_move;
                success = total_move_plans.pop_no_wait(cur_move);
                if(!success) break;

                if(cur_move->records.size() > 0){
                    outpartition << j << "\t"; // id
                    outpartition << cur_move->dest_coordinator_id << "\t"; // 
                    // outpartition << cur_move->access_frequency << "\t";    // weight
                    for(size_t i = 0 ; i < cur_move->records.size(); i ++ ){
                        outpartition << cur_move->records[i].record_key_ << "\t";
                    }
                    outpartition << "\n";
                    // total_move_plans.push_no_wait(cur_move);
                }   
                move_plans_num -- ;
                j ++ ;
            }
            outpartition.close();

            return;
        }


        template<typename T = myKeyType> 
        void update_record_degree_set(const std::vector<T>& record_keys){
            /**
             * @brief 更新权重
             * @param record_keys 递增的key
             * @note 双向图
            */
           
            // auto select_tpcc = [&](myKeyType key){
            //     // only transform STOCK_TABLE
            //     bool is_jumped = false;
            //     if(WorkloadType::which_workload == myTestSet::TPCC){
            //         uint64_t table_id = key >> RECORD_COUNT_TABLE_ID_OFFSET;
            //         if(table_id != tpcc::stock::tableID){
            //         is_jumped = true;
            //         }
            //     }
            //     return is_jumped;
            // };
            
            mm.lock();
            std::unordered_map<ycsb::ycsb::key, size_t> cache_key_coordinator_id;
            // std::unordered_set<std::pair<int, int>> remote_edges;

            for(size_t i = 0; i < record_keys.size(); i ++ ){
                // 
                auto key_one = record_keys[i];
                // if(select_tpcc(key_one)){
                //     continue;
                // }

                if (!record_degree.contains(&key_one)){
                    // [key_one -> [key_two, Node]]
                    myValueType tmp;
                    record_degree.insert(&key_one, &tmp);
                }
                myValueType* it = (myValueType*)record_degree.search_value(&key_one);

                bool is_distributed = false;
                for(size_t j = 0; j < record_keys.size(); j ++ ){
                    if(j == i)
                        continue;

                    auto key_two = record_keys[j];
                    // if(select_tpcc(key_two)){
                    //     continue;
                    // }
                    
                    if (!it->count(key_two)){
                        // [key_one -> [key_two, Node]]
                        Node n;
                        n.from = key_one;
                        n.to = key_two;
                        n.degree = 0; 

                        uint64_t key_one_table_id = key_one >> RECORD_COUNT_TABLE_ID_OFFSET;
                        uint64_t key_two_table_id = key_two >> RECORD_COUNT_TABLE_ID_OFFSET;

                        if(WorkloadType::which_workload == myTestSet::YCSB){
                            ycsb::ycsb::key key_one_real = key_one;
                            ycsb::ycsb::key key_two_real = key_two;
                            if(cache_key_coordinator_id.count(key_one_real)){
                                n.from_c_id = cache_key_coordinator_id[key_one_real];
                            } else {
                                n.from_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, key_one_table_id, &key_one_real);
                            }
                            
                            if(cache_key_coordinator_id.count(key_two_real)){
                                n.to_c_id = cache_key_coordinator_id[key_two_real];
                            } else {
                                n.to_c_id = db.get_dynamic_coordinator_id(context.coordinator_num, key_two_table_id, &key_two_real);
                            }
                        } else if(WorkloadType::which_workload == myTestSet::TPCC){
                            MoveRecord<WorkloadType> key_one_real;        
                            MoveRecord<WorkloadType> key_two_real;

                            key_one_real.set_real_key(key_one); 
                            key_two_real.set_real_key(key_two);

                            n.from_c_id = tpcc_get_coordinator_id(key_one_real); // ;db.getPartitionID(context, key_one_table_id, key_one_real);
                            n.to_c_id = tpcc_get_coordinator_id(key_two_real); // db.getPartitionID(context, key_two_table_id, key_two_real);
                        } else {
                            DCHECK(false);
                        }

                        n.on_same_coordi = n.from_c_id == n.to_c_id; 

                        it->insert(std::pair<T, Node>(key_two, n));
                        edge_nums ++ ;
                    } 

                    Node& cur_node = (*it)[key_two];
//                    VLOG(DEBUG_V12) <<"   CLAY UPDATE: " << cur_node.from << " " << cur_node.to << " " << cur_node.degree;
                    cur_node.degree += (cur_node.on_same_coordi == 1? 0: cross_txn_weight);
                    

                    // if(cur_node.on_same_coordi == 0 && test_debug == 1){
                    //     LOG(INFO) << " TEST distributed " << key_one << " " << key_two;
                    // }
                    update_hottest_edge(myTuple(cur_node.from, cur_node.from_c_id, cur_node.degree));
                    update_hottest_edge(myTuple(cur_node.to, cur_node.to_c_id, cur_node.degree));
                    

                    cache_key_coordinator_id[key_one] = cur_node.from_c_id;
                    cache_key_coordinator_id[key_two] = cur_node.to_c_id;

                    if(j > i){
                        update_node_load(cur_node);
                        if(cur_node.on_same_coordi == 0){
                            distributed_edges ++;
                            distributed_edges_on_coord[cur_node.from_c_id] ++ ;
                            distributed_edges_on_coord[cur_node.to_c_id] ++ ;
                        }
                    }
                }

                update_hottest_tuple(key_one);
                node_load[cache_key_coordinator_id[key_one]] ++;
                // if(cache_key_coordinator_id[key_one] == 0 && test_debug == 1){
                //     LOG(INFO) << " TEST " << key_one ;
                // }
                // with_coordinator_id.insert(cache_key_coordinator_id[key_one]);


            }

            // if(is_distributed){
            //     for(auto& i : with_coordinator_id){
            //         node_load[i] += cross_txn_weight;
            //     }
            // }            
            mm.unlock();
            return ;
        }
        
        void clear_graph(){
            mm.lock();
            big_node_heap.clear(); // <coordinator_id, big_heap>
            record_for_neighbor.clear();
            move_tuple_id.clear();
            hottest_tuple.clear(); // <key, frequency>
            node_load.clear(); // <coordinator_id, load>
            record_degree.clear();
            hottest_tuple_index_.clear();
            hottest_tuple_index_seq.clear();
            mm.unlock();

            init_metis_file_ = nullptr;
            init_partition_file_ = nullptr;
            partition_file_read_ptr_ = nullptr;
            // file_row_cnt_ = 0;
            test_debug ++ ;

            distributed_edges = 0;
            distributed_edges_on_coord.clear();
            edge_nums = 0;
        }
    
    private:
        std::mutex mm;

        void reset(){
            big_node_heap.clear(); // <coordinator_id, big_heap>
            record_for_neighbor.clear();
            move_tuple_id.clear();
            hottest_tuple.clear(); // <key, frequency>
            node_load.clear(); // <coordinator_id, load>
            // record_degree.clear();
        }
        void update_dest(std::shared_ptr<myMove<WorkloadType>> &move)
        {
            /**
             * @brief 更新
             * 
             */
            if (!feasible(move, move->dest_coordinator_id))
            {
                // 不可行
                auto a = get_most_related_coordinator(move);
                DCHECK(a != -1);
                if (feasible(move, a))
                {
                    move->dest_coordinator_id = a;
                }
                else
                {
                    auto l = least_loaded_partition();
                    if (move->dest_coordinator_id != l && feasible(move, l))
                    {
                        move->dest_coordinator_id = l;
                    }
                }
            }
            // 不然就说明可行，还是d
        }

        void update_hottest_edge(const myTuple& cur_node){
            // big_heap
            auto cur_partition_heap = big_node_heap.find(cur_node.c_id);
            if(cur_partition_heap == big_node_heap.end()){
                // 
                top_frequency_key<TOP_SIZE> new_big_heap;
                new_big_heap.push_back(cur_node);
                big_node_heap.insert(std::make_pair(cur_node.c_id, new_big_heap));
            } else {
                cur_partition_heap->second.push_back(cur_node);

                // std::string debug = "";
                // for(auto i = cur_partition_heap->second.begin(); i != cur_partition_heap->second.end(); i ++ ){
                //     debug += std::to_string((*i).key) + "=" + std::to_string((*i).degree) + " ";
                // }
                // LOG(INFO) << debug;

            }
        }
        void update_hottest_tuple(uint64_t key_one){
            // hottest tuple
            if(!hottest_tuple.count(key_one)){
                hottest_tuple.insert(std::make_pair(key_one, 1));

                
                hottest_tuple_index_.insert(std::make_pair(key_one, hottest_tuple_index_seq.size()));
                hottest_tuple_index_seq.push_back(key_one);
                
            } else {
                hottest_tuple[key_one] ++;
            }
        }
        void update_node_load(const Node& cur_node){
            // partition load increase
            // 
            int32_t cur_weight = cur_node.on_same_coordi ? 0 : cross_txn_weight;
            // 
            node_load[cur_node.from_c_id] += cur_weight;
            node_load[cur_node.to_c_id] += cur_weight;
        }
        
        int32_t initial_dest_partition(const std::shared_ptr<myMove<WorkloadType>> &move) {
            // 
            int32_t min_delta;
            int32_t coordinator_id=-1;
            
            bool is_first = false;
            for(auto it = node_load.begin(); it != node_load.end(); it ++ ){
                int32_t cur_dest_pd = it->first;
                if(move->dest_coordinator_id == cur_dest_pd){
                    // 其他分区！
                    continue;
                }
                if(is_first == false){
                    min_delta = cost_delta_for_receiver(move, cur_dest_pd);
                    coordinator_id = cur_dest_pd;
                    is_first = true;
                } else {
                    int32_t delta = cost_delta_for_receiver(move, cur_dest_pd);
                    if(min_delta > delta){
                        min_delta = delta;
                        coordinator_id = cur_dest_pd;
                    }
                }
            }

            return coordinator_id;
        }
        void get_neighbor_cached(myKeyType key) {
            /**
             * @brief get the neighbor of tuple [key] in degree DESC sequence
             * @param key description
             */

            // if (record_for_neighbor.find(key) == record_for_neighbor.end())
            // {
            //     //
            if(move_tuple_id.count(key)){
                return;
            }
            move_tuple_id.insert(key);
            myValueType* val = (myValueType*)record_degree.search_value(&key);
            std::vector<std::pair<myKeyType, Node> > name_score_vec(val->begin(), val->end());

            for(auto& i : name_score_vec){
                if(!move_tuple_id.count(i.second.to)){
                    move_tuple_id.insert(i.second.to);
                    q_.push(i.second);
                }
            }
            //     std::sort(name_score_vec.begin(), name_score_vec.end(), 
            //             [=](const std::pair<myKeyType, Node> &p1, const std::pair<myKeyType, Node> &p2)
            //               {
            //                   return p1.second.degree > p2.second.degree;
            //               });

            //     record_for_neighbor[key] = name_score_vec;
            // }
        }
        int32_t get_most_related_coordinator(const std::shared_ptr<myMove<WorkloadType>> &move){
            /**
             * @brief 
             * 
             */
            std::map<int32_t, int32_t> coordinator_related;
            int32_t coordinator_id = -1;
            int32_t coordinator_degree = -1;

            for(auto it = move->records.begin(); it != move->records.end(); it ++ ){
                // 遍历每一条record
                auto key = it->record_key_;
                myValueType* all_edges = (myValueType*)record_degree.search_value(&key);
                // 边权重
                for(auto itt = all_edges->begin(); itt != all_edges->end(); itt ++ ) {
                    Node& cur_n = itt->second;
                    // DCHECK(it->src_coordinator_id == cur_n.from_c_id);

                    // int32_t cur_weight = 1;
                    // if(cur_n.to_c_id != cur_n.from_c_id){
                    //     cur_weight = cross_txn_weight;
                    // }

                    if(coordinator_related.find(cur_n.to_c_id) == coordinator_related.end()){
                        coordinator_related.insert(std::make_pair(cur_n.to_c_id, cur_n.degree )); // * cur_weight
                    } else {
                        coordinator_related[cur_n.to_c_id] += cur_n.degree ; // * cur_weight
                    }
                    // 更新最密切的
                    if(coordinator_related[cur_n.to_c_id] > coordinator_degree){
                        coordinator_degree = coordinator_related[cur_n.to_c_id];
                        coordinator_id = cur_n.to_c_id;
                    }
                }
            }

            return coordinator_id;
        }
        int32_t find_overloaded_node(int32_t &average_load){
            /**
             * @brief 根据平均值，找到overloaded的partition
             * @return -1
             * 
             */
            int32_t node_num = 0;
            int32_t overloaded_coordinator_id = -1;

            // node_id, node_load
            if(node_load.size() <= 0){
                return overloaded_coordinator_id;
            }
            std::vector<std::pair<uint64_t, uint64_t>> name_score_vec(node_load.begin(), node_load.end());
            std::sort(name_score_vec.begin(), name_score_vec.end(),
                      [=](const std::pair<uint64_t, uint64_t> &p1, const std::pair<uint64_t, uint64_t> &p2)
                      {
                          return p1.second > p2.second;
                      });

            for (auto it = node_load.begin(); it != node_load.end(); it++)
            {
                average_load += it->second;
                node_num++;
            }
            average_load = average_load / node_num;


            for (auto it = name_score_vec.begin(); it != name_score_vec.end(); it++){
                if (it->second > average_load && big_node_heap[it->first].size() > 0){
                    overloaded_coordinator_id = it->first;
                    break;
                }
            }

            return overloaded_coordinator_id;
        }

        void get_most_co_accessed_neighbor(MoveRecord<WorkloadType> &new_move_rec)
        {
            /**
             * @brief 找node中与 M 权重最大的边
             * 
             */
            // for (auto it = record_for_neighbor.begin(); it != record_for_neighbor.end(); it++)
            // {
            //     // 遍历每一条 move_rec
            //     const std::vector<std::pair<myKeyType, Node> > &cur = it->second;
            //     for (auto itt = cur.begin(); itt != cur.end(); itt++)
            //     {
            //         // 遍历tuple的所有neighbor
            //         const Node &cur_node = itt->second;
            //         if(cur_node.degree <= node.degree)
            //         {
            //             break;
            //         }
            //         else if (move_tuple_id.find(cur_node.to) == move_tuple_id.end())
            //         {
            //             // 没有找到 因为cur 已经降序排过了，所以错了就直接退出
            //             node = cur_node;
            //             break;
            //         }
            //     }
            // }
            
            auto node = q_.top();
            q_.pop();

            new_move_rec.set_real_key(node.to);
            new_move_rec.src_coordinator_id = node.to_c_id;

            return;
        }

        bool move_has_neighbor(const std::shared_ptr<myMove<WorkloadType>> &move)
        {
            /**
             * @brief find if current move has uncontained tuple
             * 
             */
            bool ret = false;
            while(!q_.empty()){
                
            }
            // for (auto it = move->records.begin(); it != move->records.end(); it++)
            // {
            //     // 遍历每一条record
            //     const MoveRecord<WorkloadType> &cur_rec = *it;
            //     auto key = cur_rec.record_key_;

            //     std::vector<std::pair<myKeyType, Node> > &all_neighbors = record_for_neighbor[key];
            //     for (auto itt = all_neighbors.begin(); itt != all_neighbors.end(); itt++)
            //     {
            //         if (move_tuple_id.find(itt->first) == move_tuple_id.end())
            //         {
            //             // 当前的值不在move里，说明还有neighbor
            //             ret = true;
            //             break;
            //         }
            //     }
            //     if (ret)
            //     {
            //         break;
            //     }
            // }
            return ret;
        }

        bool feasible(const std::shared_ptr<myMove<WorkloadType>> &move, const int32_t &dest_coordinator_id)
        {
            //
            int32_t delta = cost_delta_for_receiver(move, dest_coordinator_id);
            int32_t dest_new_load = node_load[dest_coordinator_id] + delta;
            bool  not_overload = ( dest_new_load < average_load );
            bool  minus_delta  = ( delta < 0 );
            return not_overload || minus_delta;
            // true;
        }
        int32_t cost_delta_for_receiver(const std::shared_ptr<myMove<WorkloadType>> &move, const int32_t &dest_coordinator_id)
        {
            int32_t cost = 0;
            for(size_t i = 0 ; i < move->records.size(); i ++ ){
                const MoveRecord<WorkloadType>& cur = move->records[i]; 
                auto key = cur.record_key_;
                // 点权重
                if(cur.src_coordinator_id == dest_coordinator_id) continue;
                cost += hottest_tuple[key];
                myValueType* all_edges = (myValueType*)record_degree.search_value(&key);
                // 边权重
                for(auto it = all_edges->begin(); it != all_edges->end(); it ++ ) {
                    if(it->second.to_c_id == dest_coordinator_id){
                        cost -= it->second.degree; // * cross_txn_weight;
                    } else {
                        cost += it->second.degree; //  * cross_txn_weight;
                    }
                }
            }
            return cost;
        }

        int32_t cost_delta_for_sender(const std::shared_ptr<myMove<WorkloadType>> &move, const int32_t &src_coordinator_id){
            /**
             * @brief 
             * @param src_coordinator_id 当前overload的coordinator_id
             * 
             */

            int32_t cost = 0;
            for(size_t i = 0 ; i < move->records.size(); i ++ ){
                const MoveRecord<WorkloadType>& cur = move->records[i]; 
                auto key = cur.record_key_;
                if(cur.src_coordinator_id == move->dest_coordinator_id){
                    continue;
                }
                // 点权重
                cost -= hottest_tuple[key];
                myValueType* all_edges = (myValueType*)record_degree.search_value(&key);
                // 边权重
                for(auto it = all_edges->begin(); it != all_edges->end(); it ++ ) {
                    // 
                    if(it->second.from_c_id == src_coordinator_id){
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
            int32_t coordinator_id;
            int32_t coordinator_degree;

            for(auto it = node_load.begin(); it != node_load.end(); it ++ ){
                if(it == node_load.begin()){
                    coordinator_degree = it->second;
                    coordinator_id = it->first;
                }
                // 
                if(it->second < coordinator_degree){
                    coordinator_degree = it->second;
                    coordinator_id = it->first;
                }
            }
            return coordinator_id;
        }

        int32_t tpcc_get_coordinator_id(const MoveRecord<WorkloadType>& r_k){
            int32_t ret = -1;
            switch (r_k.table_id){
                case tpcc::warehouse::tableID:
                ret = db.get_dynamic_coordinator_id(context.coordinator_num, r_k.table_id, &r_k.key.w_key); 
                // db.getPartitionID(context,);
                break;
                case tpcc::district::tableID:
                ret = db.get_dynamic_coordinator_id(context.coordinator_num, r_k.table_id, &r_k.key.d_key); 
                // db.getPartitionID(context, r_k.table_id, r_k.key.d_key);
                break;
                case tpcc::customer::tableID:
                ret = db.get_dynamic_coordinator_id(context.coordinator_num, r_k.table_id, &r_k.key.c_key); 
                // db.getPartitionID(context, r_k.table_id, r_k.key.c_key);
                break;
                case tpcc::stock::tableID:
                ret = db.get_dynamic_coordinator_id(context.coordinator_num, r_k.table_id, &r_k.key.s_key); 
                //= db.getPartitionID(context, r_k.table_id, r_k.key.s_key);
                break;
            }
            return ret;
        }

        int32_t average_load;
        int32_t cur_load;

        const Context &context;
        DatabaseType& db;
        std::atomic<uint32_t> &worker_status;
        // std::vector<myMove<WorkloadType>> moves_last_round;
        ShareQueue<std::shared_ptr<simpleTransaction>, 4096> transactions_queue;
    public:
        ShareQueue<std::shared_ptr<myMove<WorkloadType>>> move_plans;

        ShareQueue<std::shared_ptr<myMove<WorkloadType>>> total_move_plans;
        
        std::atomic<bool> movable_flag;

        std::unordered_map<uint64_t, top_frequency_key<TOP_SIZE>> big_node_heap; // <coordinator_id, big_heap>
        // std::unordered_map<uint64_t, fixed_priority_queue> big_node_heap; // <coordinator_id, big_heap>
        std::unordered_map<myKeyType, std::vector<std::pair<myKeyType, Node> > > record_for_neighbor;
        std::unordered_set<myKeyType> move_tuple_id;
        std::unordered_map<myKeyType, uint64_t> hottest_tuple; // <key, frequency>


        std::unordered_map<myKeyType, uint64_t> hottest_tuple_index_;
        std::vector<myKeyType> hottest_tuple_index_seq;

        std::map<int32_t, long long> node_load; // <coordinator_id, load>
        std::map<int32_t, long long> node_load_test; // <coordinator_id, load>
        Table<100860, myKeyType, myValueType> record_degree; // unordered_ unordered_

        int test_debug = 0;
        MoveRecord<WorkloadType> new_move_rec;

        struct Cmp{
            bool operator()(Node& a, Node& b){
                return a.degree < b.degree;
            }
        };

        std::priority_queue<Node, std::vector<Node>, Cmp> q_;

        // 
        char* init_metis_file_;
        char* metis_file_read_ptr_;
        int file_size_;
        // int file_row_cnt_;
        char *saveptr_;


        // 
        char* init_partition_file_ = nullptr;
        char* partition_file_read_ptr_ = nullptr;
        int partition_file_size_;
        int partition_file_row_cnt_;
        char *partition_saveptr_;

        // std::set<uint64_t> record_key_set_;
        uint64_t edge_nums;
        int distributed_edges = 0;
        std::map<int, int> distributed_edges_on_coord;
    };
}