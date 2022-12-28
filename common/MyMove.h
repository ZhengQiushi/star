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

#include <glog/logging.h>

namespace star
{
    template <class Workload>
    struct MoveRecord{
        using myKeyType = uint64_t;
        using WorkloadType = Workload;

        int32_t table_id;
        int32_t key_size;
        int32_t field_size;
        int32_t src_coordinator_id;
        myKeyType record_key_;

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

        myMove(){
            reset(); 
        }
        void reset()
        {
            records.clear();
            dest_coordinator_id = -1;
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
    template<std::size_t N>
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
        
        void start(){
            // main loop
            ExecutorStatus status;
            size_t batch_size = 20;
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
                }

                
            }
            // not end here!
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
        
        void init_with_history(const std::string& workload_path, int start_timestamp, int end_timestamp){
            // int num_samples = 250000;
            // std::string input_path = "/home/star/data/result_test.xls";
            // std::string output_path = "/home/star/data/my_graph";
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
                            int64_t ts_ = atof(per_key_);
                            if(ts_ > end_timestamp){
                                is_break = true;
                            }
                            if(ts_ < start_timestamp || ts_ > end_timestamp){
                                is_in_range_ = false;
                                break;
                            } 
                            is_in_range_ = true;
                        } else {
                            int64_t key_ = atoi(per_key_);
                            keys.push_back(key_);
                        }
                        col_cnt_ ++ ;
                        if(col_cnt_ > 10){
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
            

            std::unordered_map<int64_t, int32_t> key_coordinator_cache;

            for(size_t i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){                
                int64_t key = hottest_tuple_index_seq[i];

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
            idx_t nParts = context.coordinator_num * 1000;   // 子图个数≥2
            idx_t objval;                           // 目标函数值
            std::vector<idx_t> parts(nVertices, 0); // 划分结果

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
                int64_t key_ = hottest_tuple_index_seq[i];
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
        

        void metis_partiion_read_from_file(const std::string& partition_filename){
            /***
             * 
            */
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
                int row_id = 0;
                auto metis_move = std::make_shared<myMove<WorkloadType>>();
                char *per_key_ = strtok_r(tmp_line_, "\t", &saveptr_);
                while(per_key_ != NULL){
                    if(col_cnt_ == 0){
                        row_id = atoi(per_key_);
                    } else {
                        int64_t key_ = atoi(per_key_);

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
        
        
        void trace_graph(const std::string& path){

            std::ofstream outfile_excel_index;
            outfile_excel_index.open(path+".index", std::ios::trunc); // ios::trunc


            std::ofstream outfile_excel;
            outfile_excel.open(path, std::ios::trunc); // ios::trunc
            
            // int64_t key = context.partition_num * context.keysPerPartition;
            int64_t vertex_num = hottest_tuple.size();

            outfile_excel << vertex_num << " " << edge_nums << "                        \n";

            for(int i = 0 ; i < hottest_tuple_index_seq.size(); i ++ ){
                int64_t key = hottest_tuple_index_seq[i];
                int64_t key_index = i + 1;

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
        void find_clump() // std::vector<myMove<WorkloadType>> &moves
        {
            /***
             * @brief find all the records to move
             *        each moves is the set of records which may from different 
             *        partition but will be transformed to the same destination
            */
            std::lock_guard<std::mutex> l(mm);
            average_load = 0;
            int32_t overloaded_coordinator_id = find_overloaded_node(average_load);
            if(overloaded_coordinator_id == -1){
                return;
            }
            // check if there is the hottest tuple on this partition
            if (big_node_heap.find(overloaded_coordinator_id) == big_node_heap.end()){
                return;
            }
            cur_load = node_load[overloaded_coordinator_id];

            //
            std::shared_ptr<myMove<WorkloadType>> C_move(new myMove<WorkloadType>()); 
            std::shared_ptr<myMove<WorkloadType>> tmp_move(new myMove<WorkloadType>());
            
            // 
            auto tuple_ptr = big_node_heap[overloaded_coordinator_id].begin();
            int32_t look_ahead = find_clump_look_ahead;

            // the cur_load will change as the clump keeps expand
            while (cur_load + cost_delta_for_sender(C_move, overloaded_coordinator_id) > average_load){
                
                if (tmp_move->records.empty()){
                    // hottest tuple
                    if (tuple_ptr == big_node_heap[overloaded_coordinator_id].end()){ 
                        // overloaded_coordinator_id 上面的内容
                        break;
                    }

                    myTuple cur_node = *tuple_ptr;
                    do {
                        cur_node = *tuple_ptr;
                        tuple_ptr++;
                        if(move_tuple_id.find(cur_node.key) == move_tuple_id.end()){
                            // have not moved yet
                            break;
                        }
                    } while(tuple_ptr != big_node_heap[overloaded_coordinator_id].end());

                    if(move_tuple_id.find(cur_node.key) != move_tuple_id.end()){
                        // all heap has been used 
                        // if(moves.size() == 0){
                        //     LOG(INFO) << "why none";
                        // }
                        break;
                    }

                    MoveRecord<WorkloadType> rec;
                    if (cur_node.c_id == overloaded_coordinator_id){
                        rec.set_real_key(cur_node.key);
                        rec.src_coordinator_id = cur_node.c_id;
                    }
                    tmp_move->records.push_back(rec);
                    tmp_move->dest_coordinator_id = overloaded_coordinator_id;

                    // dest-partition
                    tmp_move->dest_coordinator_id = initial_dest_partition(tmp_move);// tmp_move->dest_coordinator_id = rec.src_coordinator_id == cur_node.from_c_id ? cur_node.to_c_id : cur_node.from_c_id;
                    DCHECK(tmp_move->dest_coordinator_id != -1);

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
                    tmp_move->records.push_back(new_move_rec);

                    // after add new tuple, the dest may change
                    update_dest(tmp_move);

                    get_neighbor_cached(new_move_rec.record_key_);
                    move_tuple_id.insert(new_move_rec.record_key_);
                }
                else
                {
                    if (C_move->records.empty())
                    {
                        // something was wrong
                        return;
                    }
                    else
                    {
                        // start to move
                        //!TODO 先只move一次
                        move_plans.push_no_wait(C_move);
                        cur_load += cost_delta_for_sender(C_move, overloaded_coordinator_id);
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
                    move_plans.push_no_wait(C_move);
                    cur_load += cost_delta_for_sender(C_move, overloaded_coordinator_id);
                    C_move.reset(new myMove<WorkloadType>());
                    tmp_move.reset(new myMove<WorkloadType>());
                    look_ahead = find_clump_look_ahead;
                    continue;
                }
            }
            return;
        }

        template<typename T = myKeyType> 
        void update_record_degree_set(const std::vector<T>& record_keys){
            /**
             * @brief 更新权重
             * @param record_keys 递增的key
             * @note 双向图
            */
            auto select_tpcc = [&](myKeyType key){
                // only transform STOCK_TABLE
                bool is_jumped = false;
                if(WorkloadType::which_workload == myTestSet::TPCC){
                    int32_t table_id = key >> RECORD_COUNT_TABLE_ID_OFFSET;
                    if(table_id != tpcc::stock::tableID){
                    is_jumped = true;
                    }
                }
                return is_jumped;
            };
            
            mm.lock();
            std::unordered_map<ycsb::ycsb::key, size_t> cache_key_coordinator_id;

            for(size_t i = 0; i < record_keys.size(); i ++ ){
                // 
                auto key_one = record_keys[i];
                if(select_tpcc(key_one)){
                    continue;
                }

                if (!record_degree.contains(&key_one)){
                    // [key_one -> [key_two, Node]]
                    myValueType tmp;
                    record_degree.insert(&key_one, &tmp);
                }
                myValueType* it = (myValueType*)record_degree.search_value(&key_one);

                for(size_t j = 0; j < record_keys.size(); j ++ ){
                    if(j == i)
                        continue;

                    auto key_two = record_keys[j];
                    if(select_tpcc(key_two)){
                        continue;
                    }
                    
                    if (!it->count(key_two)){
                        // [key_one -> [key_two, Node]]
                        Node n;
                        n.from = key_one;
                        n.to = key_two;
                        n.degree = 0; 

                        int32_t key_one_table_id = key_one >> RECORD_COUNT_TABLE_ID_OFFSET;
                        int32_t key_two_table_id = key_two >> RECORD_COUNT_TABLE_ID_OFFSET;

                        if(WorkloadType::which_workload == myTestSet::YCSB){
                            ycsb::ycsb::key key_one_real = key_one;
                            ycsb::ycsb::key key_two_real = key_two;
                            if(cache_key_coordinator_id.count(key_one_real)){
                                n.from_c_id = cache_key_coordinator_id[key_one_real];
                            } else {
                                size_t coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, key_one_table_id, &key_one_real);
                                cache_key_coordinator_id[key_one_real] = coordinator_id_;
                                n.from_c_id = coordinator_id_;
                            }
                            
                            if(cache_key_coordinator_id.count(key_two_real)){
                                n.to_c_id = cache_key_coordinator_id[key_two_real];
                            } else {
                                size_t coordinator_id_ = db.get_dynamic_coordinator_id(context.coordinator_num, key_two_table_id, &key_two_real);
                                cache_key_coordinator_id[key_two_real] = coordinator_id_;
                                n.to_c_id = coordinator_id_;
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

                    // auto itt = it->find(key_two);

                    // myValueType* val = (myValueType*)record_degree.search_value(&key_one);
                    Node& cur_node = (*it)[key_two];
//                    VLOG(DEBUG_V12) <<"   CLAY UPDATE: " << cur_node.from << " " << cur_node.to << " " << cur_node.degree;
                    cur_node.degree += (cur_node.on_same_coordi == 1? 1: 50);

                    update_hottest_edge(myTuple(cur_node.from, cur_node.from_c_id, cur_node.degree));
                    update_hottest_edge(myTuple(cur_node.to, cur_node.to_c_id, cur_node.degree));

                    update_node_load(cur_node);      
                }

                update_hottest_tuple(key_one);

            
            }
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
                top_frequency_key<50000> new_big_heap;
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
        void update_hottest_tuple(int32_t key_one){
            // hottest tuple
            auto cur_key = hottest_tuple.find(key_one);
            if(cur_key == hottest_tuple.end()){
                hottest_tuple.insert(std::make_pair(key_one, 1));

                
                hottest_tuple_index_.insert(std::make_pair(key_one, hottest_tuple_index_seq.size()));
                hottest_tuple_index_seq.push_back(key_one);
                
            } else {
                cur_key->second ++;
            }
        }
        void update_node_load(const Node& cur_node){
            // partition load increase
            // 
            int32_t cur_weight = cur_node.on_same_coordi ? 1 : cross_txn_weight;
            // 
            auto cur_key_pd = node_load.find(cur_node.from_c_id);
            if (cur_key_pd == node_load.end()) {
                //
                node_load.insert(std::make_pair(cur_node.from_c_id, cur_weight));
            } else {
                cur_key_pd->second += cur_weight;
            }

            cur_key_pd = node_load.find(cur_node.to_c_id);
            if (cur_key_pd == node_load.end()) {
                //
                node_load.insert(std::make_pair(cur_node.to_c_id, cur_weight));
            } else {
                cur_key_pd->second += cur_weight;
            }
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
            if (record_for_neighbor.find(key) == record_for_neighbor.end())
            {
                //
                myValueType* val = (myValueType*)record_degree.search_value(&key);
                std::vector<std::pair<myKeyType, Node> > name_score_vec(val->begin(), val->end());
                std::sort(name_score_vec.begin(), name_score_vec.end(), 
                        [=](const std::pair<myKeyType, Node> &p1, const std::pair<myKeyType, Node> &p2)
                          {
                              return p1.second.degree > p2.second.degree;
                          });

                record_for_neighbor[key] = name_score_vec;
            }
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
            std::vector<std::pair<int32_t, int32_t>> name_score_vec(node_load.begin(), node_load.end());
            std::sort(name_score_vec.begin(), name_score_vec.end(),
                      [=](const std::pair<int32_t, int32_t> &p1, const std::pair<int32_t, int32_t> &p2)
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
                if (it->second > average_load){
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
            for (auto it = move->records.begin(); it != move->records.end(); it++)
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

        bool feasible(const std::shared_ptr<myMove<WorkloadType>> &move, const int32_t &dest_coordinator_id)
        {
            //
            int32_t delta = cost_delta_for_receiver(move, dest_coordinator_id);
            int32_t dest_new_load = node_load[dest_coordinator_id] + delta;
            bool  not_overload = ( dest_new_load < average_load );
            bool  minus_delta  = ( delta <= 0 );
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
        group_commit::ShareQueue<std::shared_ptr<simpleTransaction>, 4096> transactions_queue;
    public:
        group_commit::ShareQueue<std::shared_ptr<myMove<WorkloadType>>> move_plans;
        
        std::atomic<bool> movable_flag;

        std::unordered_map<int32_t, top_frequency_key<50000>> big_node_heap; // <coordinator_id, big_heap>
        // std::unordered_map<int32_t, fixed_priority_queue> big_node_heap; // <coordinator_id, big_heap>
        std::unordered_map<myKeyType, std::vector<std::pair<myKeyType, Node> > > record_for_neighbor;
        std::unordered_set<myKeyType> move_tuple_id;
        std::unordered_map<myKeyType, int32_t> hottest_tuple; // <key, frequency>


        std::unordered_map<myKeyType, int32_t> hottest_tuple_index_;
        std::vector<myKeyType> hottest_tuple_index_seq;

        std::map<int32_t, int32_t> node_load; // <coordinator_id, load>
        Table<100860, myKeyType, myValueType> record_degree; // unordered_ unordered_


        // 
        char* init_metis_file_;
        char* metis_file_read_ptr_;
        int file_size_;
        int file_row_cnt_;
        char *saveptr_;


        // 
        char* init_partition_file_;
        char* partition_file_read_ptr_;
        int partition_file_size_;
        int partition_file_row_cnt_;
        char *partition_saveptr_;

        // std::set<uint64_t> record_key_set_;
        int64_t edge_nums;
    };
}