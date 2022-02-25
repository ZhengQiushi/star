#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>

#include <map>

#include <glog/logging.h>

namespace star
{
    template <typename key_type, typename value_type>
    struct MoveRecord
    {
        key_type key;
        int32_t key_size;
        value_type value;
        int32_t field_size;
        // int32_t commit_tid; 可以不用?
        // may come from different partition but to the same dest
        int32_t src_partition_id;

        void set_key(key_type k, int32_t src_p_id){
            key = k;
            src_partition_id = src_p_id;
        }
    };

    template <typename key_type, typename value_type>
    struct myMove
    {   
    /**
     * @brief a bunch of record that needs to be transformed 
     * 
     */
        std::vector<MoveRecord<key_type, value_type> > records;
        // may come from different partition but to the same dest
        int32_t dest_partition_id;

        void reset()
        {
            records.clear();
            dest_partition_id = -1;
        }
    };

    static const int32_t cross_txn_weight = 50;
    static const int32_t find_clump_look_ahead = 5;

    struct Node
    {
        int32_t from, to;
        int32_t from_p_id, to_p_id;
        int degree;
        int on_same_coordi;
    };
    struct myTuple{
        int32_t key;
        int32_t p_id;
        int32_t degree;
        myTuple(int32_t key_, int32_t p_id_, int32_t degree_){
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
    };

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
            auto end = this->end();

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

    template <typename KeyType, typename ValueType>
    class Clay
    {
        using record_degree_bit = u_int64_t;
    public: // unordered_  unordered_
        Clay(std::unordered_map<record_degree_bit, std::unordered_map<record_degree_bit, Node> >& record_d):
        record_degree(record_d){
        }

        void find_clump(std::vector<myMove<KeyType, ValueType> > &moves)
        {
            /***
             * @brief 
            */
            average_load = 0;
            int32_t overloaded_partition_id = find_overloaded_partition(average_load);
            if(overloaded_partition_id == -1){
                return;
            }
            if (big_node_heap.find(overloaded_partition_id) == big_node_heap.end())
            {
                return;
            }
            cur_load = load_partition[overloaded_partition_id];

            //
            myMove<KeyType, ValueType> C_move, tmp_move;
            
            auto cur_node_ptr = big_node_heap[overloaded_partition_id].begin();
            
            int32_t look_ahead = find_clump_look_ahead;
            // the cur_load will change as the clump keeps expand
            while (cur_load + cost_delta_for_sender(C_move, overloaded_partition_id) > average_load)
            {
                if (tmp_move.records.empty())
                {
                    // hottest tuple
                    int32_t key = -1;
                    if (cur_node_ptr == big_node_heap[overloaded_partition_id].end())
                    { // find next hottest edge!
                        
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

                    MoveRecord<KeyType, ValueType> rec;
                    if (cur_node.p_id == overloaded_partition_id){
                        key = cur_node.key;
                        rec.set_key(cur_node.key, cur_node.p_id);
                    }
                    tmp_move.records.push_back(rec);
                    tmp_move.dest_partition_id = overloaded_partition_id;

                    // dest-partition
                    tmp_move.dest_partition_id = initial_dest_partition(tmp_move);// tmp_move.dest_partition_id = rec.src_partition_id == cur_node.from_p_id ? cur_node.to_p_id : cur_node.from_p_id;
                    DCHECK(tmp_move.dest_partition_id != -1);

                    // get its neighbor cached
                    get_neighbor_cached(key);
                    // 当前move的tuple_id
                    move_tuple_id.insert(key);
                }
                else if (move_has_neighbor(tmp_move))
                {
                    // continue to expand the clump until it is offload underneath the average level
                    MoveRecord<KeyType, ValueType> new_move_rec;
                    get_most_co_accessed_neighbor(new_move_rec);
                    tmp_move.records.push_back(new_move_rec);

                    // after add new tuple, the dest may change
                    update_dest(tmp_move);

                    get_neighbor_cached(*(int32_t*)& new_move_rec.key);
                    move_tuple_id.insert(*(int32_t*)&new_move_rec.key);
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
            
                if(feasible(tmp_move, tmp_move.dest_partition_id)){
                    C_move = tmp_move;
                } else if(!C_move.records.empty()) {
                    look_ahead -= 1;
                }

                if(look_ahead == 0){
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
        void update_dest(myMove<KeyType, ValueType> &move)
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
        int32_t initial_dest_partition(const myMove<KeyType, ValueType> &move) {
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
        void get_neighbor_cached(int32_t key) {
            /**
             * @brief get the neighbor of tuple [key] in degree DESC sequence
             * @param key description
             */
            if (record_for_neighbor.find(key) == record_for_neighbor.end())
            {
                //
                std::vector<std::pair<int32_t, Node> > name_score_vec(record_degree[key].begin(), record_degree[key].end());
                std::sort(name_score_vec.begin(), name_score_vec.end(), 
                        [=](const std::pair<int32_t, Node> &p1, const std::pair<int32_t, Node> &p2)
                          {
                              return p1.second.degree > p2.second.degree;
                          });

                record_for_neighbor.insert(std::make_pair(key, name_score_vec));
            }
        }
        int32_t get_most_related_partition(const myMove<KeyType, ValueType> &move){
            /**
             * @brief 
             * 
             */
            std::map<int32_t, int32_t> partition_related;
            int32_t partition_id = -1;
            int32_t partition_degree = -1;

            for(auto it = move.records.begin(); it != move.records.end(); it ++ ){
                // 遍历每一条record
                int32_t key = *(int32_t*)& it->key;
                std::unordered_map<record_degree_bit, Node>& all_edges = record_degree[key];
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

        void get_most_co_accessed_neighbor(MoveRecord<KeyType, ValueType> &new_move_rec)
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
                const std::vector<std::pair<int32_t, Node> > &cur = it->second;
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

            new_move_rec.key = node.to;
            new_move_rec.src_partition_id = node.to_p_id;

            return;
        }

        bool move_has_neighbor(const myMove<KeyType, ValueType> &move)
        {
            /**
             * @brief find if current move has uncontained tuple
             * 
             */
            bool ret = false;
            for (auto it = move.records.begin(); it != move.records.end(); it++)
            {
                // 遍历每一条record
                const MoveRecord<KeyType, ValueType> &cur_rec = *it;
                int32_t key = *(int32_t *)&cur_rec.key;

                std::vector<std::pair<int32_t, Node> > &all_neighbors = record_for_neighbor[key];
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

        bool feasible(const myMove<KeyType, ValueType> &move, const int32_t &dest_partition_id)
        {
            //
            int32_t delta = cost_delta_for_receiver(move, dest_partition_id);
            int32_t dest_new_load = load_partition[dest_partition_id] + delta;
            bool  not_overload = ( dest_new_load < average_load );
            bool  minus_delta  = ( delta <= 0 );
            return not_overload || minus_delta;
            // true;
        }
        int32_t cost_delta_for_receiver(const myMove<KeyType, ValueType> &move, const int32_t &dest_partition_id)
        {
            int32_t cost = 0;
            for(size_t i = 0 ; i < move.records.size(); i ++ ){
                const MoveRecord<KeyType, ValueType>& cur = move.records[i]; 
                auto key = *(int32_t*)&cur.key;
                // 点权重
                cost += hottest_tuple[key];
                std::unordered_map<record_degree_bit, Node>& all_edges = record_degree[key];
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

        int32_t cost_delta_for_sender(const myMove<KeyType, ValueType> &move, const int32_t &src_partition_id)
        {
            /**
             * @brief 
             * @param src_partition_id 当前overload的partition
             */

            int32_t cost = 0;
            for(size_t i = 0 ; i < move.records.size(); i ++ ){
                const MoveRecord<KeyType, ValueType>& cur = move.records[i]; 
                auto key = *(int32_t*)&cur.key;
                if(cur.src_partition_id == move.dest_partition_id){
                    // 没动？
                    continue;
                }
                // 点权重
                cost -= hottest_tuple[key];
                std::unordered_map<record_degree_bit, Node>& all_edges = record_degree[key];
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
        std::unordered_map<record_degree_bit, std::vector<std::pair<int32_t, Node> > > record_for_neighbor;
        std::unordered_set<record_degree_bit> move_tuple_id;
        std::unordered_map<record_degree_bit, int32_t> hottest_tuple; // <key, frequency>
        std::map<int32_t, int32_t> load_partition; // <partition_id, load>

        std::unordered_map<record_degree_bit, std::unordered_map<record_degree_bit, Node> >& record_degree; // unordered_ unordered_

    };
}