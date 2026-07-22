use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct ReconnectCounter(AtomicU64);

impl ReconnectCounter  {
    pub fn bump(&self){
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn take(&self) -> u64{
        self.0.swap(0, Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct QdbStats{
    pub last_flush_micros: AtomicU64,
    pub rows_flushed: AtomicU64,
}
