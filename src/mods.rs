pub mod init_connect {
    include!(concat!(env!("OUT_DIR"), "/init_connect.rs"));
}

pub mod qot_common {
    include!(concat!(env!("OUT_DIR"), "/qot_common.rs"));
}

pub mod qot_sub {
    include!(concat!(env!("OUT_DIR"), "/qot_sub.rs"));
}

pub mod qot_update_basic_qot {
    include!(concat!(env!("OUT_DIR"), "/qot_update_basic_qot.rs"));
}

pub mod qot_update_ticker {
    include!(concat!(env!("OUT_DIR"), "/qot_update_ticker.rs"));
}

pub mod qot_get_market_state {
    include!(concat!(env!("OUT_DIR"), "/qot_get_market_state.rs"));
}