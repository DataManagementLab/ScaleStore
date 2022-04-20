#pragma once

struct warehouse_t {
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      Integer w_id;

      bool operator==(const Key& other) { return w_id == other.w_id; }
      bool operator>(const Key& other) const { return w_id > other.w_id; }
      bool operator>=(const Key& other) const { return w_id >= other.w_id; }
      bool operator<(const Key& other) const { return w_id < other.w_id; }
   };
   Varchar<10> w_name;
   Varchar<20> w_street_1;
   Varchar<20> w_street_2;
   Varchar<20> w_city;
   Varchar<2> w_state;
   Varchar<9> w_zip;
   Numeric w_tax;
   Numeric w_ytd;
   uint8_t padding[1024];  // fix for contention; could be solved with contention split from
                           // http://cidrdb.org/cidr2021/papers/cidr2021_paper21.pdf
};

struct district_t {
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      Integer d_w_id;
      Integer d_id;

      bool operator==(const Key& other) { return (d_w_id == other.d_w_id) && (d_id == other.d_id); }
      bool operator>(const Key& other) const {
         if (d_w_id > other.d_w_id) return true;
         if (d_w_id < other.d_w_id) return false;
         return (d_id > other.d_id);  // equal
      }
      
      bool operator<(const Key& other) const {
         if (d_w_id < other.d_w_id) return true;
         if (d_w_id > other.d_w_id) return false;
         return (d_id < other.d_id);  // equal
      }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Varchar<10> d_name;
   Varchar<20> d_street_1;
   Varchar<20> d_street_2;
   Varchar<20> d_city;
   Varchar<2> d_state;
   Varchar<9> d_zip;
   Numeric d_tax;
   Numeric d_ytd;
   Integer d_next_o_id;
};

struct customer_t {
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      Integer c_w_id;
      Integer c_d_id;
      Integer c_id;

      bool operator==(const Key& other) { return (c_w_id == other.c_w_id) && (c_d_id == other.c_d_id) && (c_id == other.c_id); }

      bool operator<(const Key& other) const {
         if (c_w_id < other.c_w_id) return true;
         if (c_w_id > other.c_w_id) return false;
         // equal c_w_id
         if (c_d_id < other.c_d_id) return true;
         if (c_d_id > other.c_d_id) return false;
         // equal
         return (c_id < other.c_id);  // equal
      }
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Varchar<16> c_first;
   Varchar<2> c_middle;
   Varchar<16> c_last;
   Varchar<20> c_street_1;
   Varchar<20> c_street_2;
   Varchar<20> c_city;
   Varchar<2> c_state;
   Varchar<9> c_zip;
   Varchar<16> c_phone;
   Timestamp c_since;
   Varchar<2> c_credit;
   Numeric c_credit_lim;
   Numeric c_discount;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   Numeric c_delivery_cnt;
   Varchar<500> c_data;
};

struct customer_wdl_t {
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      Integer c_w_id;
      Integer c_d_id;
      Varchar<16> c_last;
      Varchar<16> c_first;

      bool operator==(const Key& other) const{
         return (c_w_id == other.c_w_id) && (c_d_id == other.c_d_id) && (c_last == other.c_last) && (c_first == other.c_first);
      }

      bool operator<(const Key& other) const  {
         if (c_w_id < other.c_w_id) return true;
         if (c_w_id > other.c_w_id) return false;

         if (c_d_id < other.c_d_id) return true;
         if (c_d_id > other.c_d_id) return false;

         if (c_last < other.c_last) return true;
         if (c_last > other.c_last) return false;

         return (c_first < other.c_first);
      }
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Integer c_id;
};

struct history_t {
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      Integer thread_id;
      Integer h_pk;

      bool operator==(const Key& other) const {
         return (thread_id == other.thread_id) && (h_pk == other.h_pk);
      }

      bool operator<(const Key& other) const {
         if (thread_id < other.thread_id) return true;
         if (thread_id > other.thread_id) return false;
         return (h_pk < other.h_pk);
      }
      
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Integer h_c_id;
   Integer h_c_d_id;
   Integer h_c_w_id;
   Integer h_d_id;
   Integer h_w_id;
   Timestamp h_date;
   Numeric h_amount;
   Varchar<24> h_data;
};

struct neworder_t {
   static constexpr int id = 5;
   struct Key {
      static constexpr int id = 5;
      Integer no_w_id;
      Integer no_d_id;
      Integer no_o_id;
      
      bool operator==(const Key& other) const {
         return (no_w_id == other.no_w_id) && (no_d_id == other.no_d_id) && (no_o_id == other.no_o_id);
      }
      
      bool operator<(const Key& other) const {
         if (no_w_id < other.no_w_id) return true;
         if (no_w_id > other.no_w_id) return false;
         // equal c_w_id
         if (no_d_id < other.no_d_id) return true;
         if (no_d_id > other.no_d_id) return false;
         // equal
         return (no_o_id < other.no_o_id);  // equal
      }
      
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
};

struct order_t {
   static constexpr int id = 6;
   struct Key {
      static constexpr int id = 6;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_id;

      bool operator==(const Key& other) const { return (o_w_id == other.o_w_id) && (o_d_id == other.o_d_id) && (o_id == other.o_id); }

      bool operator<(const Key& other) const {
         if (o_w_id < other.o_w_id) return true;
         if (o_w_id > other.o_w_id) return false;
         // equal c_w_id
         if (o_d_id < other.o_d_id) return true;
         if (o_d_id > other.o_d_id) return false;
         // equal
         return (o_id < other.o_id);  // equal
      }
      
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Integer o_c_id;
   Timestamp o_entry_d;
   Integer o_carrier_id;
   Numeric o_ol_cnt;
   Numeric o_all_local;
};

struct order_wdc_t {
   static constexpr int id = 7;
   struct Key {
      static constexpr int id = 7;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_c_id;
      Integer o_id;
      
      bool operator==(const Key& other) const{
         return (o_w_id == other.o_w_id) && (o_d_id == other.o_d_id) && (o_c_id == other.o_c_id) && (o_id == other.o_id);
      }

      bool operator<(const Key& other) const  {
         if (o_w_id < other.o_w_id) return true;
         if (o_w_id > other.o_w_id) return false;

         if (o_d_id < other.o_d_id) return true;
         if (o_d_id > other.o_d_id) return false;

         if (o_c_id < other.o_c_id) return true;
         if (o_c_id > other.o_c_id) return false;
         return (o_id < other.o_id);
      }
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
};

struct orderline_t {
   static constexpr int id = 8;
   struct Key {
      static constexpr int id = 8;
      Integer ol_w_id;
      Integer ol_d_id;
      Integer ol_o_id;
      Integer ol_number;

      bool operator==(const Key& other) const{
         return (ol_w_id == other.ol_w_id) && (ol_d_id == other.ol_d_id) && (ol_o_id == other.ol_o_id) && (ol_number == other.ol_number);
      }

      bool operator<(const Key& other) const  {
         if (ol_w_id < other.ol_w_id) return true;
         if (ol_w_id > other.ol_w_id) return false;

         if (ol_d_id < other.ol_d_id) return true;
         if (ol_d_id > other.ol_d_id) return false;

         if (ol_o_id < other.ol_o_id) return true;
         if (ol_o_id > other.ol_o_id) return false;
         return (ol_number < other.ol_number);
      }
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Integer ol_i_id;
   Integer ol_supply_w_id;
   Timestamp ol_delivery_d;
   Numeric ol_quantity;
   Numeric ol_amount;
   Varchar<24> ol_dist_info;
};

struct item_t {
   static constexpr int id = 9;
   struct Key {
      static constexpr int id = 9;
      Integer i_id;

      bool operator==(const Key& other) { return i_id == other.i_id; }
      bool operator>(const Key& other) const { return i_id > other.i_id; }
      bool operator>=(const Key& other) const { return i_id >= other.i_id; }
      bool operator<(const Key& other) const { return i_id < other.i_id; }
   };
   Integer i_im_id;
   Varchar<24> i_name;
   Numeric i_price;
   Varchar<50> i_data;
};

struct stock_t {
   static constexpr int id = 10;
   struct Key {
      static constexpr int id = 10;
      Integer s_w_id;
      Integer s_i_id;
      
      
      bool operator==(const Key& other) const{
         return (s_w_id == other.s_w_id) && (s_i_id == other.s_i_id);
      }

      bool operator<(const Key& other) const  {
         if (s_w_id < other.s_w_id) return true;
         if (s_w_id > other.s_w_id) return false;
         return (s_i_id < other.s_i_id);
      }
      bool operator>(const Key& other) const { return (other < *this); }
      bool operator>=(const Key& other) const { return !(*this < other); }
   };
   Numeric s_quantity;
   Varchar<24> s_dist_01;
   Varchar<24> s_dist_02;
   Varchar<24> s_dist_03;
   Varchar<24> s_dist_04;
   Varchar<24> s_dist_05;
   Varchar<24> s_dist_06;
   Varchar<24> s_dist_07;
   Varchar<24> s_dist_08;
   Varchar<24> s_dist_09;
   Varchar<24> s_dist_10;
   Numeric s_ytd;
   Numeric s_order_cnt;
   Numeric s_remote_cnt;
   Varchar<50> s_data;
};

// -------------------------------------------------------------------------------------
// output operator
std::ostream& operator<<(std::ostream& os, item_t c) {
   os << "item: " << c.i_im_id << " " << c.i_name.toString();
   return os;
}
