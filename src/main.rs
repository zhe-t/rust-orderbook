use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;
use std::fmt::{Debug, Result};
use std::iter::Rev;
use std::iter::{IntoIterator, Iterator};
use std::vec::Vec;

#[derive(Clone, Debug)]
pub struct Order {
    pub id: u64,
    pub price: u64,
    pub volume: u32,
    pub side: i8
}
pub struct PriceBucket {
    pub price_level: u64,
    orders: Vec<Order>
}
pub struct Execution {
    pub volume: u32,
    pub buy_order_id: u64,
    pub sell_order_id: u64
}
impl fmt::Debug for PriceBucket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PriceBucket {{ price_level: {}, orders: {:?} }}", self.price_level, self.orders)
    }
}
#[derive(Debug)]
pub struct LimitOrderBook {
    id_wheel: u64,
    ask_book: AskBook,
    bid_book: BidBook,
}
impl Debug for AskBook {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "AskBook {{ price_buckets: {:?} }}", self.price_buckets)
    }
}
impl Debug for BidBook {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "BidBook {{ price_buckets: {:?} }}", self.price_buckets)
    }
}
pub trait OrderManager {
    fn add_order(&mut self, order: Order) -> Vec<Execution>;
    fn remove_order(&mut self, order: Order);
}
pub trait BestPrice {
    fn best_price(&self) -> u64;
}
pub trait OrderBook: BestPrice + OrderManager + PriceBucketIter {}
impl OrderBook for AskBook {}
impl OrderBook for BidBook {}
impl OrderManager for PriceBucket {
    fn add_order(&mut self, order: Order) -> Vec<Execution> {
        self.orders.push(order);
        Vec::new()
    }
    fn remove_order(&mut self, order: Order) {
        let idx = self.orders.iter().position(|x| x.id == order.id);
        if idx.is_some() {
            self.orders.remove(idx.unwrap());
        }
    }
}
impl PriceBucket {
    pub fn from_price(price_level: u64) -> PriceBucket {
        PriceBucket {
            price_level: price_level,
            orders: Vec::new(),
        }
    }
    pub fn from_order(order: Order) -> PriceBucket {
        PriceBucket {
            price_level: order.price,
            orders: vec![order],
        }
    }
    pub fn volume(&self) -> u32 {
        self.orders.iter().map(|x| x.volume).sum()
    }
}

#[macro_export]
macro_rules! expand_book_struct {
    ($book_struct_name : ident) => {
        pub struct $book_struct_name {
            price_buckets: BTreeMap<u64, PriceBucket>
        }
        impl $book_struct_name {
            pub fn new() -> $book_struct_name {
                $book_struct_name {
                    price_buckets: BTreeMap::new()
                }
            }
            pub fn volume_at_price_level(&self, price: u64) -> u32 {
                if let Some(b) = self.price_buckets.get(&price) {
                    b.volume()
                } else {
                    0
                }
            }
        }
        impl OrderManager for $book_struct_name {
            fn add_order(&mut self, order: Order) -> Vec<Execution> {
                if let Some(bucket) = self.price_buckets.get_mut(&order.price) {
                    bucket.add_order(order);
                } else {
                    let price = order.price;
                    let price_bucket = PriceBucket::from_order(order);
                    self.price_buckets.insert(price, price_bucket);
                }
                return Vec::new();
            }
            fn remove_order(&mut self, order: Order) {
                if let Some(bucket) = self.price_buckets.get_mut(&order.price) {
                    bucket.remove_order(order);
                }
            }
        }
    };
}
expand_book_struct!(BidBook);
expand_book_struct!(AskBook);
pub enum IterVariant<'a> {
    AskBookIter(btree_map::IterMut<'a, u64, PriceBucket>),
    BidBookIter(Rev<btree_map::IterMut<'a, u64, PriceBucket>>),
    None,
}
pub trait PriceBucketIter {
    fn iter_mut(&mut self) -> IterVariant;
}
impl PriceBucketIter for AskBook {
    fn iter_mut(&mut self) -> IterVariant {
        IterVariant::AskBookIter(self.price_buckets.iter_mut())
    }
}
impl PriceBucketIter for BidBook {
    fn iter_mut(&mut self) -> IterVariant {
        IterVariant::BidBookIter(self.price_buckets.iter_mut().rev())
    }
}
impl BestPrice for AskBook {
    fn best_price(&self) -> u64 {
        *self.price_buckets.keys().nth(0).unwrap_or(&0)
    }
}
impl BestPrice for BidBook {
    fn best_price(&self) -> u64 {
        *self.price_buckets.keys().last().unwrap_or(&0)
    }
}
impl LimitOrderBook {
    pub fn new() -> LimitOrderBook {
        LimitOrderBook {
            id_wheel: 0,
            ask_book: AskBook::new(),
            bid_book: BidBook::new(),
        }
    }
    pub fn best_bid(&self) -> u64 {
        return self.bid_book.best_price();
    }
    pub fn best_ask(&self) -> u64 {
        return self.ask_book.best_price();
    }
    pub fn ask_volume_at_price_level(&self, price: u64) -> u32 {
        self.ask_book.price_buckets.get(&price).unwrap_or(&PriceBucket::from_price(price)).volume()
    }
    pub fn bid_volume_at_price_level(&self, price: u64) -> u32 {
        self.bid_book.price_buckets.get(&price).unwrap_or(&PriceBucket::from_price(price)).volume()
    }
    fn next_id(&mut self) -> u64 {
        self.id_wheel += 1;
        self.id_wheel
    }
    fn check_and_do_cross_spread_walk<B1: OrderBook, B2: OrderBook>(
        mut order: Order,
        book: &mut B1,
        opp_book: &mut B2,
        func: fn(u64, u64) -> bool,
    ) -> Vec<Execution> {
        let mut executions = Vec::new();
        // check if order is crossing spread
        if opp_book.best_price() > 0 && func(order.price, opp_book.best_price()) {
            let (residual_volume, orders_to_remove, execs) =
                LimitOrderBook::cross_spread_walk(&mut order, opp_book, func);
            order.volume = residual_volume;
            executions.extend(execs);
            for o in orders_to_remove {
                opp_book.remove_order(o);
            }
        }
        // If order.volume is still positive, it means the order was partially filled or not filled at all.
        // In that case, add the remaining volume to the book.
        if order.volume > 0 {
            let _ = book.add_order(order);
        }
        executions
    }
    

    fn cross_spread_walk<B: OrderBook>(
        order: &mut Order,
        book: &mut B,
        func: fn(u64, u64) -> bool,
    ) -> (u32, Vec<Order>, Vec<Execution>) {
        let mut volume = order.volume;
        let mut orders_to_remove: Vec<Order> = Vec::new();
        let mut executions: Vec<Execution> = Vec::new();
    
        let price_bucket_iter = book.iter_mut();
    
        let it: Box<dyn Iterator<Item = (&u64, &mut PriceBucket)>> = match price_bucket_iter {
            IterVariant::AskBookIter(x) => Box::new(x.into_iter()),
            IterVariant::BidBookIter(y) => Box::new(y.into_iter()),
            _ => unimplemented!(),
        };
    
        for bucket_order in it.flat_map(|x| x.1.orders.iter_mut()) {
            if !(volume > 0 && func(order.price, bucket_order.price)) {
                break;
            }
    
            let mut buy_order_id = 0;
            let mut sell_order_id = 0;
            let mut executed_volume = 0;
    
            if bucket_order.side == 1 {
                buy_order_id = bucket_order.id;
                sell_order_id = order.id;
            } else if bucket_order.side == -1 {
                buy_order_id = order.id;
                sell_order_id = bucket_order.id;
            }
    
            if volume >= bucket_order.volume {
                println!(
                    "Taking {} from order id {}, left {}",
                    bucket_order.volume,
                    bucket_order.id,
                    volume - bucket_order.volume
                );
                volume -= bucket_order.volume;
                executed_volume = bucket_order.volume;
                bucket_order.volume = 0;
    
                // Only remove the order if its volume is zero
                if bucket_order.volume == 0 {
                    orders_to_remove.push(bucket_order.clone());
                }
            } else {
                bucket_order.volume -= volume;
                executed_volume = volume;
                volume = 0;
            }
            executions.push(Execution {
                volume: executed_volume,
                buy_order_id: buy_order_id,
                sell_order_id: sell_order_id,
            });
        }
    
        // return orders_to_remove to make borrow checker happy.
        // we can do book.remove_order(o) here without compiler complaining.
        (volume, orders_to_remove, executions)
    }
    

    pub fn ask_iter(&mut self) -> btree_map::IterMut<u64, PriceBucket> {
        self.ask_book.price_buckets.iter_mut()
    }

    pub fn bid_iter(&mut self) -> btree_map::IterMut<u64, PriceBucket> {
        self.bid_book.price_buckets.iter_mut()
    }
}
impl OrderManager for LimitOrderBook {
    fn add_order(&mut self, mut order: Order) -> Vec<Execution> {
        order.id = self.next_id();
        let mut executions;
        if order.side == -1 {
            executions = LimitOrderBook::check_and_do_cross_spread_walk(
                order,
                &mut self.ask_book,
                &mut self.bid_book,
                |x, y| x <= y,
            );
        } else {
            executions = LimitOrderBook::check_and_do_cross_spread_walk(
                order,
                &mut self.bid_book,
                &mut self.ask_book,
                |x, y| x >= y,
            );
        }
        executions
    }
    fn remove_order(&mut self, order: Order) {
        if order.side == -1 {
            self.ask_book.remove_order(order)
        } else {
            self.bid_book.remove_order(order)
        }
    }
}

fn main() {
    let mut order_book = LimitOrderBook::new();
    let order_1 = Order {
        id: 1,
        price: 100,
        volume: 10,
        side: 1,
    };
    let order_2 = Order {
        id: 2,
        price: 100,
        volume: 20,
        side: 1,
    };
    let order_3 = Order {
        id: 3,
        price: 101,
        volume: 15,
        side: 1,
    };
    order_book.add_order(order_1);
    order_book.add_order(order_2);
    order_book.add_order(order_3);
    println!("Order book before market order:\n{:#?}", order_book);
    let order_4 = Order {
        id: 4,
        price: 100,
        volume: 25,
        side: -1,
    };
    println!("Order book after market order 4:\n{:#?}", order_book);
    let order_5 = Order {
        id: 5,
        price: 100,
        volume: 5,
        side: -1,
    };
    order_book.add_order(order_4);
    order_book.add_order(order_5);
    println!("Order book after market order 5:\n{:#?}", order_book);
    let order_6 = Order {
        id: 6,
        price: 100,
        volume: 25,
        side: -1,
    };
    order_book.add_order(order_6);
    println!("Order book after market order 6:\n{:#?}", order_book);
}
