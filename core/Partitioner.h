//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <glog/logging.h>
#include <memory>
#include <numeric>
#include <string>


#include "Table.h"
namespace star {

class Partitioner {
public:
  Partitioner(std::size_t coordinator_id, std::size_t coordinator_num) {
    DCHECK(coordinator_id < coordinator_num);
    this->coordinator_id = coordinator_id;
    this->coordinator_num = coordinator_num;
  }

  virtual ~Partitioner() = default;

  std::size_t total_coordinators() const { return coordinator_num; }

  virtual std::size_t replica_num() const = 0;

  virtual bool is_replicated() const = 0;

  virtual bool has_master_partition(std::size_t partition_id) const = 0;

  virtual std::size_t master_coordinator(std::size_t partition_id) const = 0;

  virtual std::size_t secondary_coordinator(std::size_t partition_id) const = 0;

  
  virtual bool is_partition_replicated_on(std::size_t partition_id,
                                          std::size_t coordinator_id) const = 0;

  bool is_partition_replicated_on_me(std::size_t partition_id) const {
    return is_partition_replicated_on(partition_id, coordinator_id);
  }

  
  virtual bool has_master_partition(int table_id, int partition_id, const void* key) const = 0;

  virtual bool is_dynamic() const = 0;

  virtual std::size_t master_coordinator(int table_id, int partition_id, const void* key) const = 0;

  // check if the replica of `key` is on Node `coordinator_id` or not
  virtual bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const = 0;


  virtual bool is_backup() const = 0;

protected:
  std::size_t coordinator_id;
  std::size_t coordinator_num;
};

/*
 * N is the total number of replicas.
 * N is always larger than 0.
 * The N coordinators from the master coordinator have the replication for a
 * given partition.
 */

template <std::size_t N> class HashReplicatedPartitioner : public Partitioner {
public:
  HashReplicatedPartitioner(std::size_t coordinator_id,
                            std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(N > 0 && N <= coordinator_num);
  }

  ~HashReplicatedPartitioner() override = default;

  std::size_t replica_num() const override { return N; }

  bool is_replicated() const override { return N > 1; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_num;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id < coordinator_num);
    std::size_t first_replica = master_coordinator(partition_id);
    std::size_t last_replica = (first_replica + N - 1) % coordinator_num;

    if (last_replica >= first_replica) {
      return first_replica <= coordinator_id && coordinator_id <= last_replica;
    } else {
      return coordinator_id >= first_replica || coordinator_id <= last_replica;
    }
  }


  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    // DCHECK(false);
    return master_coordinator(partition_id) == coordinator_id;// false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    // DCHECK(false);
    return master_coordinator(partition_id); // false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    // DCHECK(false);
    return is_partition_replicated_on(partition_id, coordinator_id); //false;
  }

  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };
};

using HashPartitioner = HashReplicatedPartitioner<1>;

class PrimaryBackupPartitioner : public Partitioner {
public:
  PrimaryBackupPartitioner(std::size_t coordinator_id,
                           std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num == 2);
  }

  ~PrimaryBackupPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return coordinator_id == 0;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return 0;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }

  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id < coordinator_num);
    return true;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return coordinator_id == 1; }

  bool is_dynamic() const override { return false; };

};

/*
 * There are 2 replicas in the system with N coordinators.
 * Coordinator 0 has a full replica.
 * The other replica is partitioned across coordinator 1 and coordinator N - 1
 *
 *
 * The master partition is partition id % N.
 *
 * case 1
 * If the master partition is from coordinator 1 to coordinator N - 1,
 * the secondary partition is on coordinator 0.
 *
 * case 2
 * If the master partition is on coordinator 0,
 * the secondary partition is from coordinator 1 to coordinator N - 1.
 *
 */

class StarSPartitioner : public Partitioner {
public:
  StarSPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num >= 2);
  }

  ~StarSPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_num;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id < coordinator_num);

    auto master_id = master_coordinator(partition_id);
    auto secondary_id = 0u; // case 1
    if (master_id == 0) {
      secondary_id = partition_id % (coordinator_num - 1) + 1; // case 2
    }
    return coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };

};

class StarCPartitioner : public Partitioner {
public:
  StarCPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num >= 2);
  }

  ~StarCPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return coordinator_id == 0;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return 0;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id < coordinator_num);

    if (coordinator_id == 0)
      return true;
    // 非全副本节点需要判断
    if(partition_id % coordinator_num == 0){
      return coordinator_id == (partition_id / coordinator_num) % (coordinator_num - 1) + 1;
    } else {
      return coordinator_id == partition_id % coordinator_num;
                              //partition_id % (coordinator_num - 1) + 1; 
                              //((partition_id - 1) % (coordinator_num - 1)) + 1;
    }
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return false; };
};

/***
 * Lion Partitioner
 * Features:
 *      - 2 full replicas
 *      - 2 kinds of replicas: + (stands for dynamic replica | master    ), 
 *                             - (stands for static replica  | secondary )
 *      - replicas will change as time goes by accordding to the locality.
 * Here is an example(inital state):
 * 
 *     | N0     |  N1    |  N2    |
 * P0  | +-     |        |        |
 * P1  |        |  +-    |        |
 * P2  |        |        |  +-    |
 * P3  | +-     |        |        |
 * P4  |        |  +-    |        |
 * P5  |        |        |  +-    |
 * 
 * ==> only the dynamic replica can be moved 
 * 
 *     | N0     |  N1    |  N2    |
 * P0  | +-     |        |        |
 * P1  | +      |   -    |        |
 * P2  |        |        |  +-    |
 * P3  |  -     |        |  +     |
 * P4  |        |  +-    |        |
 * P5  |        |        |  +-    | 
 * 
*/

class LionInitPartitioner : public Partitioner {
public:
  LionInitPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num >= 2);
  }

  ~LionInitPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    return (partition_id) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(coordinator_id < coordinator_num);

    auto master_id = master_coordinator(partition_id);
    auto secondary_id = secondary_coordinator(partition_id); // case 1
    
    return coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };

};


template <class Workload> 
class LionDynamicPartitioner : public LionInitPartitioner {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  LionDynamicPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, DatabaseType& db):
    LionInitPartitioner(coordinator_id, coordinator_num), db(db){
    CHECK(coordinator_num >= 2);
  }

  ~LionDynamicPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    return master_coordinator(table_id, partition_id, key) == coordinator_id;
  }

  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    /**
     * @brief dynamic mastership. master piece will be moved to other coordinators!
     * 
     */
    // if(secondary_coordinator(partition_id) != coordinator_id){
    //   // not static replica, always master!
    //   return coordinator_id;
    // } else {
    // static replica, check if is re-mastered
    
    // auto table_router = db.find_router_table(table_id, partition_id);
    // size_t* master_coordinator = (size_t*)table_router->search_value(key);
    //  == coordinator_id;
    return db.get_dynamic_coordinator_id(coordinator_num, table_id, key);// *master_coordinator;
    // if(std::get<1>(ret->dynamic_dst) == 1){
    //   //
    //   return std::get<0>(ret->dynamic_dst);
    // } else {
    //   return std::get<0>(ret->static_dst);
    // }
    // }
  }

  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(coordinator_id < coordinator_num);
      // static replica
      return secondary_coordinator(partition_id) == coordinator_id ; // || 
            //  master_coordinator(table_id, partition_id, key);
    // } else {
    //   // 
    //   // auto table_router = db.find_router_table(table_id, partition_id);
    //   // RTable* ret = (RTable*)table_router->search_value(key);
    //   // return std::get<0>(ret->dynamic_dst) == coordinator_id || 
    //   //       std::get<0>(ret->static_dst) == coordinator_id ;

    //   auto table_router = db.find_router_table(table_id, partition_id);
    //   size_t* master_coordinator = (size_t*)table_router->search_value(key);
    //   return (*master_coordinator) == coordinator_id;
    // }
  }


  bool has_master_partition(std::size_t partition_id) const override {
    DCHECK(false);
    return false; // master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    // DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(false);    
    return true;// coordinator_id == master_id || coordinator_id == secondary_id;
  }
  
  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return true; };

private:
  DatabaseType& db;
};

template <class Workload> 
class LionStaticPartitioner : public LionInitPartitioner {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;

  LionStaticPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, DatabaseType& db):
    LionInitPartitioner(coordinator_id, coordinator_num), db(db){
    CHECK(coordinator_num >= 2);
  }

  ~LionStaticPartitioner() override = default;

  std::size_t replica_num() const override { return 2; }

  bool is_replicated() const override { return true; }

  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    return master_coordinator(table_id, partition_id, key) == coordinator_id;
  }

  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    // the static replica is master at initial state
    return (partition_id) % coordinator_num;
  }

  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    // check if has dynamic replica on coordinator
    // size_t i = ;
    
    // first should on the same-node 
  
    // then should 

    // DCHECK(coordinator_id < coordinator_num);
    // if(has_master_partition(table_id, partition_id, key) == true){
    //   return true;
    // }
    // auto table_router = db.find_router_table(table_id, partition_id);
    // size_t* master_coordinator = (size_t*)table_router->search_value(key);
    // return (*master_coordinator) == coordinator_id;

    // auto table_router = db.find_router_table(table_id, partition_id);
    // RTable* ret = (RTable*)table_router->search_value(key);
    // return std::get<0>(ret->dynamic_dst) == coordinator_id ; // || 
           // std::get<0>(ret->static_dst) == coordinator_id ;
    return db.get_dynamic_coordinator_id(coordinator_num, table_id, key) == coordinator_id;
  }


  bool has_master_partition(std::size_t partition_id) const override {
    DCHECK(false);
    return false; // master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return 0; // (partition_id + 1) % coordinator_num;
  }

  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    // the static replica is master at initial state
    DCHECK(false);
    return 0; // (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // judge if it is replicated partition or not
    DCHECK(false);    
    return true;// coordinator_id == master_id || coordinator_id == secondary_id;
  }

  bool is_backup() const override { return coordinator_id != 0; }

  bool is_dynamic() const override { return false; };

private:
  DatabaseType& db;
};

class CalvinPartitioner : public Partitioner {

public:
  CalvinPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                    std::vector<std::size_t> replica_group_sizes)
      : Partitioner(coordinator_id, coordinator_num) {

    std::size_t size = 0;
    for (auto i = 0u; i < replica_group_sizes.size(); i++) {
      CHECK(replica_group_sizes[i] > 0);
      size += replica_group_sizes[i];

      if (coordinator_id < size) {
        coordinator_start_id = size - replica_group_sizes[i];
        replica_group_id = i;
        replica_group_size = replica_group_sizes[i];
        break;
      }
    }
    CHECK(std::accumulate(replica_group_sizes.begin(),
                          replica_group_sizes.end(), 0u) == coordinator_num);
  }

  ~CalvinPartitioner() override = default;

  std::size_t replica_num() const override { return replica_group_size; }

  bool is_replicated() const override {
    // replica group in calvin is independent
    return false;
  }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % replica_group_size + coordinator_start_id;
  }
  std::size_t secondary_coordinator(std::size_t partition_id) const override {
    DCHECK(false);
    return (partition_id) % coordinator_num;
  }
  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    // replica group in calvin is independent
    return false;
  }
  
  bool has_master_partition(int table_id, int partition_id, const void* key) const override { // std::size_t partition_id, 
    DCHECK(false);
    return false;
  }

  
  std::size_t master_coordinator(int table_id, int partition_id, const void* key) const override {
    DCHECK(false);
    return false;

  }

  
  bool is_partition_replicated_on(int table_id, int partition_id, const void* key,
                                  std::size_t coordinator_id) const override {
    DCHECK(false);
    return false;
  }
  bool is_backup() const override { return false; }

  bool is_dynamic() const override { return false; };


public:
  std::size_t replica_group_id;
  std::size_t replica_group_size;

private:
  // the first coordinator in this replica group
  std::size_t coordinator_start_id;
};

class PartitionerFactory {
public:
  static std::unique_ptr<Partitioner>
  create_partitioner(const std::string &part, std::size_t coordinator_id,
                     std::size_t coordinator_num) {

    if (part == "hash") {
      return std::make_unique<HashPartitioner>(coordinator_id, coordinator_num);
    } else if (part == "hash2") {
      return std::make_unique<HashReplicatedPartitioner<2>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash3") {
      return std::make_unique<HashReplicatedPartitioner<3>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash4") {
      return std::make_unique<HashReplicatedPartitioner<4>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash5") {
      return std::make_unique<HashReplicatedPartitioner<5>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash6") {
      return std::make_unique<HashReplicatedPartitioner<6>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash7") {
      return std::make_unique<HashReplicatedPartitioner<7>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "hash8") {
      return std::make_unique<HashReplicatedPartitioner<8>>(coordinator_id,
                                                            coordinator_num);
    } else if (part == "pb") {
      return std::make_unique<PrimaryBackupPartitioner>(coordinator_id,
                                                        coordinator_num);
    } else if (part == "StarS") {
      return std::make_unique<StarSPartitioner>(coordinator_id,
                                                coordinator_num);
    } else if (part == "StarC") {
      return std::make_unique<StarCPartitioner>(coordinator_id,
                                                coordinator_num);
    } else if (part == "Lion") {
      return std::make_unique<LionInitPartitioner>(coordinator_id,
                                                   coordinator_num);
    } else {
      CHECK(false);
      return nullptr;
    }
  }
};

} // namespace star