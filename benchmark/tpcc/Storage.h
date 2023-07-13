//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/tpcc/Schema.h"
#include "benchmark/tpcc/Workload.h"

namespace star {
namespace tpcc {

    struct Record{
        using myKeyType = uint64_t;

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

        Record(){
            table_id = 0;
            // memset(&key, 0, sizeof(key));
            // memset(&value, 0, sizeof(value));
        }
        ~Record(){
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
            return;

        }

        void set_real_value(uint64_t record_key, const void* record_val){            
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


struct Storage {
  warehouse::key warehouse_key;
  warehouse::value warehouse_value;

  district::key district_key;
  district::value district_value;

  customer_name_idx::key customer_name_idx_key;
  customer_name_idx::value customer_name_idx_value;

  customer::key customer_key;
  customer::value customer_value;

  item::key item_keys[15];
  item::value item_values[15];

  stock::key stock_keys[15];
  stock::value stock_values[15];

  new_order::key new_order_key;

  order::key order_key;
  order::value order_value;

  order_line::key order_line_keys[15];
  order_line::value order_line_values[15];

  history::key h_key;
  history::value h_value;

  std::vector<Record> key_value;
  void clear() {
    key_value.clear();
  }
};
} // namespace tpcc
} // namespace star